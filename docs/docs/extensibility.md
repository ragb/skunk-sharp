# Extensibility

The DSL's vocabulary is `TypedExpr[T, Args]`. Everything — column references, literals,
function calls, operator results — is a `TypedExpr[T, Args]`, and every builder (SELECT
projections, WHERE predicates, ORDER BY, HAVING, SET assignments) consumes them.

Third-party modules add new operators and functions by shipping `extension` methods on
`TypedExpr[T, Args]`. No changes to core are required. The contribs that ship in-core
under `skunk.sharp.contrib.*` use exactly this surface (see [Contrib modules](contrib.md)).

## `TypedExpr` contract

```scala
trait TypedExpr[T, Args]:
  def fragment: skunk.Fragment[Args]   // typed-args SQL fragment
  def codec:    skunk.Codec[T]         // decoder for SELECT projections
```

`Args` is the captured-parameter tuple — `Void` for static / value-baked expressions,
`T` for a deferred `Param[T]`, `Concat[A, B]` for combinators. The smart-flat
`Where.Concat` collapses chains to a flat user-facing tuple at the compile boundary.

Factory helpers:

```scala
TypedExpr.lit[T](v: T)(using PgTypeFor[T]): TypedExpr[T, Void]   // inlined primitive literal
TypedExpr.raw(sql: String): TypedExpr[Nothing, Void]              // raw SQL bit
TypedExpr.parameterised[T](v: T)(using PgTypeFor[T]): TypedExpr[T, Void]  // bake a runtime value
```

## `PgFunction` and `PgOperator`

Thin wrappers so third-party modules don't reinvent function/operator construction. Each
threads input `Args` through to the output via `Where.Concat`:

```scala
object PgFunction:
  def nullary[R](name: String)(using PgTypeFor[R]): TypedExpr[R, Void]
  def unary  [A, R, X](name: String)(using PgTypeFor[R]):
    TypedExpr[A, X] => TypedExpr[R, X]
  def binary [A, B, R, X, Y](name: String)(using PgTypeFor[R]):
    (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, Where.Concat[X, Y]]

object PgOperator:
  def infix  [A, B, R, X, Y](op: String)(using PgTypeFor[R]):
    (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, Where.Concat[X, Y]]
  def prefix [A, R, X](op: String)(using PgTypeFor[R]):
    TypedExpr[A, X] => TypedExpr[R, X]
  def postfix[A, R, X](op: String)(using PgTypeFor[R]):
    TypedExpr[A, X] => TypedExpr[R, X]
```

## The `expr"..."` interpolator

For one-off snippets, `expr"..."` weaves literal SQL with `TypedExpr` interpolations and
commits to a result type via `.as[T]` or `.asCodec(...)`. The interpolator lives at
`skunk.sharp.expr` — not re-exported from `dsl`:

```scala mdoc:silent
import skunk.sharp.dsl.*
import skunk.sharp.expr
import java.util.UUID

case class User(id: UUID, email: String, age: Int)
val users = Table.of[User]("users").withPrimary("id").withDefault("id")

val ageParam = Param[Int]

// Boolean predicate — Args = Int
val ageGte: TypedExpr[Boolean, Int] =
  expr"${users.columnsView.age} >= $ageParam".as[Boolean]

// Reuse a column's exact codec instead of re-writing it
val upperEmail: TypedExpr[String, skunk.Void] =
  expr"upper(${users.columnsView.email})".asCodec(users.columnsView.email)
```

**Static-by-default**: every interpolation must be a `TypedExpr`. Bare runtime values
are rejected — pick `Param[T]` (deferred), `lit(v)` (compile-time literal), or
`Param.bind(v)` (explicit bake), just like the operator surface.

## Adding a user-defined function

No module needed — just a value:

```scala mdoc:silent
def lower[X](e: TypedExpr[String, X]): TypedExpr[String, X] =
  PgFunction.unary[String, String, X]("lower")(e)

def length[X](e: TypedExpr[String, X]): TypedExpr[Int, X] =
  PgFunction.unary[String, Int, X]("length")(e)

val q = users.select(u => lower(u.email))
             .where(u => length(u.email) > lit(5))
             .compile
```

## Writing a companion module

The pattern for a hypothetical `skunk-sharp-jsonb` module:

```scala
// 1. Opaque type + codec + PgTypeFor (with extension hint if needed)
opaque type Jsonb <: io.circe.Json = io.circe.Json
object Jsonb:
  val RequiredExtension: String = "..."  // omit if built-in
  val codec: skunk.Codec[Jsonb]  = ...
  given skunk.sharp.pg.PgTypeFor[Jsonb] =
    skunk.sharp.pg.PgTypeFor.instanceWithExtension(codec, RequiredExtension)

// 2. Extension methods on TypedExpr[Jsonb, A]
extension [A](e: skunk.sharp.TypedExpr[Jsonb, A])
  inline def ->>[B](key: skunk.sharp.TypedExpr[String, B])
      : skunk.sharp.TypedExpr[String, skunk.sharp.where.Where.Concat[A, B]] =
    skunk.sharp.PgOperator.infix[Jsonb, String, String, A, B]("->>")(e, key)

  inline def `@>`[B](other: skunk.sharp.TypedExpr[Jsonb, B])
      : skunk.sharp.where.Where[skunk.sharp.where.Where.Concat[A, B]] = {
    val expr = skunk.sharp.PgOperator.infix[Jsonb, Jsonb, Boolean, A, B]("@>")(e, other)
    skunk.sharp.where.Where(expr.fragment)
  }
```

Users `import skunk.sharp.jsonb.*` and the new operators compose naturally:

```scala
// WHERE payload ->> 'status' = 'active'
table.select.where(c => (c.payload ->> lit("status")) === lit("active"))
```

Use `instanceWithExtension(codec, "<ext-name>")` on the `PgTypeFor` so the schema
validator auto-discovers the dependency. For function-only modules without a tag (e.g.
pgcrypto), expose a `RequiredExtension: String` and document `extraExtensions =
Set(...)` for `SchemaValidator.validate`.

## Built-in extension modules

| Module | Covers |
| --- | --- |
| `skunk-sharp-iron` | [Iron](https://iltotore.github.io/iron/) refinement bridges |
| `skunk-sharp-refined` | [Refined](https://github.com/fthomas/refined) refinement bridges |
| `skunk-sharp-circe` | `json` / `jsonb` codecs via skunk-circe |
| `skunk.sharp.contrib.*` | In-core contribs: citext, ltree, hstore, pg_trgm, pgcrypto, fuzzystrmatch — see [Contrib modules](contrib.md) |
