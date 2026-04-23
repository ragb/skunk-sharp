# Extensibility

The DSL's vocabulary is `TypedExpr[T]`. Everything — column references, literals,
function calls, operator results — is a `TypedExpr[T]`, and every builder (SELECT
projections, WHERE predicates, ORDER BY, HAVING, SET assignments) consumes `TypedExpr[T]`.

Third-party modules add new operators and functions by shipping `extension` methods on
`TypedExpr[T]`. No changes to core are required.

## `TypedExpr` contract

```scala
trait TypedExpr[T]:
  def render: skunk.AppliedFragment  // what the compiler needs
  def codec:  skunk.Codec[T]         // decoder for SELECT projections
```

Factory helpers:

```scala
TypedExpr.lit(v: T)(using Codec[T]): TypedExpr[T]      // bound parameter
TypedExpr.raw(sql: String): TypedExpr[Nothing]          // raw SQL fragment
TypedExpr.apply(frag)(codec): TypedExpr[T]              // arbitrary fragment
```

## `PgFunction` and `PgOperator`

Core ships thin wrappers so third-party modules don't reinvent function/operator construction:

```scala
object PgFunction:
  def unary [A, R](name: String)(using Codec[R]): TypedExpr[A] => TypedExpr[R]
  def binary[A, B, R](name: String)(using Codec[R]): (TypedExpr[A], TypedExpr[B]) => TypedExpr[R]
  def nary  [R](name: String)(using Codec[R]): List[TypedExpr[?]] => TypedExpr[R]

object PgOperator:
  def infix[A, B, R](op: String)(using Codec[R]): (TypedExpr[A], TypedExpr[B]) => TypedExpr[R]
  def prefix[A, R](op: String)(using Codec[R]): TypedExpr[A] => TypedExpr[R]
```

## Adding a user-defined function

No module needed — just a value:

```scala mdoc:silent
import skunk.sharp.dsl.*

val lower  = PgFunction.unary[String, String]("lower")
val length = PgFunction.unary[String, Int]("length")

case class User(id: java.util.UUID, email: String, age: Int, created_at: java.time.OffsetDateTime, deleted_at: Option[java.time.OffsetDateTime])
val users = Table.of[User]("users").withPrimary("id").withDefault("id").withUnique("email")

val q = users.select(u => lower(u.email))
             .where(u => length(u.email) > 5)
             .compile
```

## Writing a companion module

The pattern for a hypothetical `skunk-sharp-jsonb` module:

```scala
// 1. Opaque type + codec + PgTypeFor
opaque type Jsonb = io.circe.Json
given skunk.Codec[Jsonb]           = ...
given skunk.sharp.pg.PgTypeFor[Jsonb] = ...

// 2. Extension methods on TypedExpr[Jsonb]
extension (e: skunk.sharp.TypedExpr[Jsonb])
  def ->>(key: String): skunk.sharp.TypedExpr[String] =
    skunk.sharp.PgOperator.infix[Jsonb, String, String]("->>")(e, skunk.sharp.TypedExpr.lit(key))

  def `@>`(other: Jsonb): skunk.sharp.TypedExpr[Boolean] =
    skunk.sharp.PgOperator.infix[Jsonb, Jsonb, Boolean]("@>")(e, skunk.sharp.TypedExpr.lit(other))
```

Users `import skunk.sharp.jsonb.given, skunk.sharp.jsonb.*` and the new operators
compose naturally:

```scala
// WHERE payload ->> 'status' = 'active'
table.select.where(cols => cols.payload ->> "status" === "active")
```

## Planned companion modules

| Module | Covers |
| --- | --- |
| `skunk-sharp-json` | `json` / `jsonb` operators (`->`, `->>`, `@>`, `?`, …) via skunk-circe |
| `skunk-sharp-refined` | [Refined](https://github.com/fthomas/refined) constraints, analogous to `skunk-sharp-iron` |
| `skunk-sharp-ltree` | Hierarchical `ltree` type and operators |
| `skunk-sharp-arrays` | Postgres array operators (`@>`, `&&`, `ANY`, …) |
| `skunk-sharp-fts` | Full-text search (`tsvector`, `tsquery`, `@@`) |
| `skunk-sharp-postgis` | PostGIS spatial types and operators |
