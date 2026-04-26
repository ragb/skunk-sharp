# Param migration — TypedExpr[T] → TypedExpr[T, Args]

Branch: `param-placeholders`. Off `macro-sql-assembly`.

Goal: collapse the captured-args mechanism. Make typed `Args` flow through every expression, not just through `Where`. `Param[T]` is the universal placeholder for "value supplied at execute time" and works wherever a `TypedExpr` is accepted.

Outcome: `.compile` returns a `QueryTemplate[Args, R]` / `CommandTemplate[Args]` whose Args type is the aggregate of every Param used anywhere in the chain. Execution methods take args at execute (`q.run(s)(args)`). `AppliedFragment` use is limited to `whereRaw` / `havingRaw` payloads and the final Skunk-side execute.

Don't try to keep things compiling incrementally. This is a rewrite.

## New core types

```
trait TypedExpr[T, Args]:
  def fragment: Fragment[Args]    // typed Fragment, NOT AppliedFragment with args baked
  def codec:    Codec[T]

final class TypedColumn[T, Null, N]      extends TypedExpr[T, skunk.Void]
final class AliasedExpr[T, N, Args]      extends TypedExpr[T, Args]
final class Param[T] (codec: Codec[T])   extends TypedExpr[T, T]

def lit[T](v: T): TypedExpr[T, skunk.Void]    // primitive literals only — strings/runtime values must use Param
```

`Where[Args]` collapses to a type alias:

```
type Where[Args] = TypedExpr[Boolean, Args]
```

`SetAssignment` collapses to:

```
final class SetAssignment[T, Args] (
  col: TypedColumn[T, ?, ?],
  rhs: TypedExpr[T, Args],
)
```

`InsertSource.TypedRow` becomes:

```
final case class TypedRow[Args](fragment: Fragment[Args])  // no captured value
```

For old `users.insert(rowNamedTuple)` ergonomics where the user has values: they wrap each value as `Param[T]` (or as `lit(v)` for primitives). Migration helper: `users.insert(row)` becomes `users.insert.values(Param[UUID], Param[String], ...)` etc. — TBD.

## Compiled forms

```
final class QueryTemplate[Args, R](
  val fragment: Fragment[Args],
  val codec:    Codec[R],
)

final class CommandTemplate[Args](
  val fragment: Fragment[Args],
)
```

Execution extensions take args at execute time:

```
extension [Args, R](q: QueryTemplate[Args, R])
  def run(session)(args: Args): F[List[R]]                = session.execute(q.typedQuery)(args)
  def unique(session)(args: Args): F[R]                   = session.unique(q.typedQuery)(args)
  def option(session)(args: Args): F[Option[R]]           = session.option(q.typedQuery)(args)
  def stream(session, chunkSize: Int = 64)(args: Args)    = session.stream(q.typedQuery)(args, chunkSize)
  def cursor(session)(args: Args)                         = session.cursor(q.typedQuery)(args)
  def prepared(session): F[PreparedQuery[F, Args, R]]     = session.prepare(q.typedQuery)
  def typedQuery: skunk.Query[Args, R]                    = q.fragment.query(q.codec)
```

`Args = skunk.Void` overload (no args needed):

```
extension [R](q: QueryTemplate[Void, R])
  def run(session): F[List[R]] = q.run(session)(Void)
  // and similarly for unique / option / stream / cursor
```

Same shape for `CommandTemplate[Args]`.

`AppliedFragment` is no longer cached on `QueryTemplate` — it doesn't make sense without args. Subquery embedding (`AsSubquery`) threads Args through composition instead.

## Operator surface

Every operator becomes a function over `TypedExpr[T, A]`:

```
extension [T, A](lhs: TypedExpr[T, A])
  def ===[B](rhs: TypedExpr[T, B])              : Where[Concat[A, B]]
  def !==[B](rhs: TypedExpr[T, B])              : Where[Concat[A, B]]
  def < [B](rhs: TypedExpr[T, B])(using Order) : Where[Concat[A, B]]
  // …
  def isNull                                    : Where[A]
  def isNotNull                                 : Where[A]
  def in[B](rhs: TypedExpr[T, B], more: TypedExpr[T, ?]*) : Where[Concat[A, ...]]
  def like[B](pat: TypedExpr[String, B])        : Where[Concat[A, B]]   // for T = String
  def between[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C]) : Where[Concat[A, Concat[B, C]]]
```

For runtime values without explicit `Param`: drop the value-taking overloads. The user wraps values explicitly:

- `u.id === Param[UUID]`               — deferred
- `u.id === lit(uuid)`                 — inline literal (only valid if uuid is a compile-time constant, blocked by lit's macro)
- `u.id === Param.bind(uuid)`          — bake a runtime value as a Void-args TypedExpr (helper for the dynamic-context path)

`Param.bind[T](v: T)(using Codec): TypedExpr[T, Void]` is the migration helper that builds a `Fragment[Void]` with the value baked via contramap — equivalent to today's captured-args path but explicit.

## PgFunction / PgOperator

`PgFunction.nullary[R]: TypedExpr[R, Void]` — unchanged shape.

`PgFunction.unary` / `binary` / `nary` thread Args from each input:

```
def unary[A, R, X](name: String): TypedExpr[A, X] => TypedExpr[R, X]
def binary[A, B, R, X, Y](name): (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, Concat[X, Y]]
def nary[A, R, …]: TypedExpr[R, joined-args]
```

Same for `PgOperator.infix`/`prefix`/`postfix`.

## Builders (SelectBuilder / InsertCommand / UpdateReady / DeleteReady)

The `WArgs` / `HArgs` / `SetArgs` type parameters stay — they're already the typed Args slots. They start at `skunk.Void` and grow via `Concat` as `.where` / `.having` / `.set` lambdas return `TypedExpr[Boolean, A]` / `TypedExpr[T, A]`.

The internal `whereOpt: Option[(Fragment[?], Any)]` slots drop the `Any` half — only `Fragment[?]` (existential) is needed since values don't ride along anymore. `assemble` builds `Fragment[Args]` directly from the typed slot fragments + structural parts.

`SelectBuilder.assemble[Args, R]` no longer needs the `preEnc.contramap` runtime args dance — every part either contributes structurally (Left AppliedFragment with Void-encoder OR plain string parts) or via a typed Fragment whose encoder composes into the final `Fragment[Args].encoder`.

## Subquery embedding

`AsSubquery[Q, T]` becomes:

```
trait AsSubquery[Q, T, Args]:
  def codec(q: Q):    Codec[T]
  def fragment(q: Q): Fragment[Args]
```

`Pg.exists(sub: SelectBuilder[..., A, ...])` returns `TypedExpr[Boolean, A]` — Args of the inner subquery propagate into the outer's WHERE / HAVING / projection wherever the EXISTS expression is placed.

Same for `notExists`, scalar `.asExpr`.

## File-by-file checklist

Order of edits (rewrite — don't try to keep things compiling between steps):

1. `TypedExpr.scala`             — change signature to `TypedExpr[T, Args]`. Update `apply`, `lit`, `parameterised`, `joined`, `cast`, `as`, `AliasedExpr`.
2. `TypedColumn.scala`           — extend `TypedExpr[T, Void]`. Drop the AppliedFragment render; expose `fragment: Fragment[Void]`.
3. `Param.scala`                 — `Param[T] extends TypedExpr[T, T]`.
4. `where/Where.scala`           — collapse to `type Where[A] = TypedExpr[Boolean, A]`. Remove the class. Move `&&` / `||` / `not` to extensions on `TypedExpr[Boolean, A]`. Keep `Where.Concat` (move to a `TypedExprArgs` helper module).
5. `where/Ops.scala` / `ops/ExprOps.scala` — every extension is now `extension [T, A](lhs: TypedExpr[T, A])` with the new Args-threading shape. Drop value-taking forms — users use `Param[T]` or `lit(v)`.
6. `internal/SqlMacros.scala`    — macros now produce `TypedExpr[T, A]`. The TypedColumn LHS fast path produces a `Fragment[A]` whose `parts` are `[Left(col.sqlRef + " op "), Right…]` and the encoder is the rhs's encoder (or product if both have args). Three macros: `infix`, `infix2`, `postfix`. Same signature.
7. `PgFunction.scala`            — `PgFunction.nullary` / `unary` / `binary` / `nary`, `PgOperator.infix` / `prefix` / `postfix` — Args-threading.
8. `pg/ArrayOps.scala`           — Args-threading on `||`, `=ANY`, etc.
9. `pg/RangeOps.scala`           — Args-threading.
10. `pg/functions/*.scala`        — every function signature gets an Args parameter. ~25 files.
11. `dsl/Compiled.scala`          — rename `CompiledQuery` → `QueryTemplate`, `CompiledCommand` → `CommandTemplate`. Drop `args` field. Update execution extensions (take Args at execute, plus Void overload). Update `AsSubquery`.
12. `dsl/Select.scala`            — `SelectBuilder[Ss, WArgs, HArgs]`'s `whereOpt` drops the Any half. `assemble` updated. `compile` returns `QueryTemplate[Concat[WArgs, HArgs], NamedRow]`. ProjectedSelect similarly. ProjectedSelect's `Proj` already carries `TypedExpr[?]` items; update to `TypedExpr[?, A]` so projection-list Args fold in too.
13. `dsl/Insert.scala`            — `InsertCommand[Cols, Args]` already exists. `InsertSource.TypedRow[Args]` (no value field). `users.insert(row)` ergonomics: `row` is a named tuple of `TypedExpr[T, A]` per column — user supplies via `Param[T]` or `lit(v)`. Args = combined.
14. `dsl/Update.scala`            — `SetAssignment[T, Args]` (no value field). Same combinator chain. UpdateReady's SetArgs threads from set-assignments' Args.
15. `dsl/Delete.scala`            — `DeleteReady[Cols, Name, Args]` — Args from `.where`. No structural changes beyond `whereOpt` shape.
16. `dsl/Join.scala`              — `SourceEntry.onPredOpt: Option[TypedExpr[Boolean, ?]]` (existential). Joining threads onPred Args into the SELECT compiler's WArgs slot.
17. `dsl/Cte.scala` / `dsl/SetOpQuery.scala` / `dsl/Values.scala` — Args plumbing for inner-fragment composition.
18. `dsl/CaseExpr.scala`          — CASE WHEN branches are TypedExprs; thread Args through each arm.
19. `internal/RawConstants.scala` — no changes; constants still apply.
20. `validation/*`                 — schema validation uses Codecs and Names; should be unaffected.

Tests:
- `core/test/scala/skunk/sharp/**/*.scala` — extensive rewrites. Migration patterns:
  - `=== value` → `=== Param[T]` and tests pass values at execute (or `=== lit(constant)` for primitives).
  - `.compile.run(session)` → `.compile.run(session)(args)` (or just `.run(session)` if Args = Void after the migration).
  - `.compile.af` SQL-string assertions stay the same — same SQL output.
- `tests/test/scala/...` (integration) — same migration patterns.

## Migration helpers

Provide `Param.bind[T](v: T)(using PgTypeFor[T]): TypedExpr[T, Void]` — bakes a runtime value into a `Fragment[Void]` via `contramap`. Equivalent to today's captured-args mode, just explicit. Used in dynamic-AF construction contexts (subquery embedding inside an outer query that has values, building AppliedFragments programmatically).

Document in CLAUDE.md that:
- `lit(v)` is for compile-time constants (primitives).
- `Param[T]` is for execute-time values (the standard static-query path).
- `Param.bind(v)` is the rare "I want a runtime value baked as Void-args right now" escape hatch, mostly for migration / interop with `whereRaw` / `havingRaw`.

## Out of scope (this PR)

- Macro that owns the entire builder chain producing a single interned `Fragment[Void]` per fully-static call site.
- Per-call-site Fragment template caching (`u.id === Param[Int]` rebuilds Fragment per `.where` invocation; could intern).
- Iron-module updates beyond what TypedExpr signature changes force.
- **Named params**: `Param[T, N <: String & Singleton]` (e.g. `Param[UUID]("id")`) so multi-param queries compile-check
  arg names at `.run(session)(named-tuple)`. Args at the QueryTemplate level become a `NamedTuple[Names, Values]`
  for the all-named case; combinator chains merge NamedTuples via Scala 3.7+ type-level ops. Phase 2 — don't block
  on the NamedTuple-merge machinery before the basic plumbing works.

## Risk / open questions

- **INSERT row ergonomics.** `users.insert((id = uuid, email = "x"))` is convenient. With `Param[T]`-only, it becomes `users.insert((id = Param[UUID], email = Param[String]))` — and the user supplies `(uuid, "x")` at `.run(session)((uuid, "x"))`. Acceptable trade-off?
- **Extension modules** (`skunk-sharp-iron`, future `-jsonb`, `-ltree`). Their TypedExpr-using signatures all change. They share the trait so the migration is mechanical, but it does break.
- **Public API**. Anyone outside this repo using `TypedExpr[T]` will need to migrate to `TypedExpr[T, Void]` or `TypedExpr[T, ?]`. Pre-1.0 so churn is acceptable.
