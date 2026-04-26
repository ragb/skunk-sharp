# Resume: Param migration (TypedExpr[T] → TypedExpr[T, Args])

**Branch**: `param-placeholders` (off `macro-sql-assembly`)
**Head**: `5c06bf4`
**Compiles**: NO. Foundation types changed; every consumer is broken until full migration lands.
**Plan**: see [`PARAM_MIGRATION.md`](PARAM_MIGRATION.md) for the full file-by-file checklist + design rationale.

## What's done (5 commits on the branch)

1. `e1de635` — migration plan doc
2. `9896df4` — `TypedExpr[T, Args]` (typed `Fragment[Args]`, not AppliedFragment), `TypedColumn` extends `TypedExpr[T, Void]`, `Param[T] extends TypedExpr[T, T]`, `type Where[A] = TypedExpr[Boolean, A]`
3. `6360d91` — `litMacro` returns `TypedExpr[T, Void]`, `ops/ExprOps.scala` rewritten with Args-threading + `Param.bind` shortcuts, **`internal/SqlMacros.scala` deleted**
4. `c8ad8dd` — `PgFunction.{nullary,unary,binary,nary}` and `PgOperator.{infix,prefix,postfix}` thread Args, `pg/functions/Shared` helpers updated, `PgAggregate` migrated
5. `5c06bf4` — `PgString` migrated

## What's pending (next session — resume in this order)

The user explicitly chose to pause pg/functions/* (which is mechanical "thread A through") and **resume with the dsl/* layer first** — that's where the user-visible API decisions surface.

### 1. dsl/Compiled.scala (priority — sets the public surface)
Rename `CompiledQuery[Args, R]` → `QueryTemplate[Args, R]` and `CompiledCommand[Args]` → `CommandTemplate[Args]`. Drop the `args: Args` field — fragment is the only carrier. Execution extensions take args at execute time:
```scala
extension [Args, R](q: QueryTemplate[Args, R])
  def run(session)(args: Args): F[List[R]]
  def unique(session)(args: Args): F[R]
  def option(session)(args: Args): F[Option[R]]
  def stream(session, chunkSize: Int = 64)(args: Args): Stream[F, R]
  def cursor(session)(args: Args): Resource[F, Cursor[F, R]]
  def prepared(session): F[PreparedQuery[F, Args, R]]
  def typedQuery: skunk.Query[Args, R] = q.fragment.query(q.codec)

// Args = Void overload — no args at execute:
extension [R](q: QueryTemplate[Void, R])
  def run(session): F[List[R]]   // delegates with Void
  // (and unique/option/stream/cursor)
```
- `.af` on QueryTemplate doesn't make sense without args — drop it. For dynamic embedding, the new path is `template.bind(args)` → AppliedFragment OR rework AsSubquery to thread Args (see point 2 below).
- Same shape on CommandTemplate.
- Update `AsSubquery[Q, T]` → `AsSubquery[Q, T, Args]` so subquery embedding threads inner args into outer composition.

### 2. dsl/Insert.scala (user-agreed design — projection then values)
Replace today's `users.insert(namedTuple)` with two-stage:
```scala
// Stage 1: pick columns
users.insert(u => (u.email, u.age))   // returns InsertWithCols[Cols, ProjectedCols]

// Stage 2: provide values
.values((email = Param[String], age = Param[Int]))   // NamedTuple of TypedExpr per col
.compile  // CommandTemplate[(email: String, age: Int)]

// Param-only static template:
val create: CommandTemplate[(email: String, age: Int)] =
  users.insert(u => (u.email, u.age)).values((email = Param[String], age = Param[Int])).compile
create.run(s)((email = "x", age = 30))

// Mixed (Param + lit):
users.insert(u => (u.email, u.age)).values((email = Param[String], age = lit(18)))
// Args = String

// Captured-value ergonomic shortcut (separate method to avoid overload ambiguity):
users.insert(u => (u.email, u.age)).valuesOf((email = "x", age = 18))
// Args = Void; values auto-Param.bind under the hood
```
**Open question still pending**: keep `users.insert((id = ..., email = ..., age = ...))` as a whole-row shortcut equivalent to `users.insert(u => allColumns).values(thatTuple)`? Probably yes for ergonomics — settle this when you reach the file.
**Open question 2**: implicit `Conversion[T, TypedExpr[T, Void]]` to allow values directly inside `.values((email = "x", ...))` (intrusive `import scala.language.implicitConversions`) vs forcing explicit `lit("x")` / `Param.bind("x")` wrap. Default: separate `.valuesOf` method.

### 3. dsl/Update.scala (similar pattern, simpler)
`SetAssignment[T, Args]` drops the `args: Args` field; carries only `Fragment[Args]`. `:=` overloads on `TypedColumn[T, ?, ?]`:
- `:=[B](rhs: TypedExpr[T, B]): SetAssignment[T, B]` — canonical (Param, lit, column ref).
- `:=(rhs: T)(using PgTypeFor[T]): SetAssignment[T, Void]` — captured-value via `Param.bind` under the hood. Same SQL output as today.
`Combined(fragment: Fragment[?], args: Any)` becomes just `Fragment[?]`. `UpdateReady[Cols, Name, SetArgs, WArgs]` Args slots already track types — just drop value storage.

### 4. dsl/Delete.scala (small)
`DeleteReady[Cols, Name, Args]` — `whereOpt` drops the `Any` half, becomes `Option[Fragment[?]]`. Compile path returns `CommandTemplate[Args]` (or QueryTemplate for RETURNING).

### 5. dsl/Select.scala (largest — 1095 lines)
- `SelectBuilder[Ss, WArgs, HArgs]`'s `whereOpt` and `havingOpt` drop their `Any` half: `Option[Fragment[?]]`.
- `assemble` no longer needs the `preEnc.contramap` runtime args dance — every part is either a static Left AppliedFragment, a `Fragment[?]` slot whose encoder composes via `.product`, or a `Fragment[Void]`. Build `Fragment[Args]` directly.
- `compile` returns `QueryTemplate[Where.Concat[WArgs, HArgs], NamedRowOf[Cols]]`.
- `compileFragment` (used by AsSubquery) needs to thread Args — either return `Fragment[Args]` or keep returning AppliedFragment with explicit args binding. **TBD**: the AsSubquery design (point 1) drives this.
- `ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row]` similarly. `Proj` items are `TypedExpr[?, ?]` so projection-list Args fold into the running typed-args slot too (was Void before because all expressions were AF-baked).

### 6. dsl/Join.scala (medium)
`SourceEntry.onPredOpt: Option[TypedExpr[Boolean, ?]]` (existential `Where`). The SELECT compiler threads onPred Args into its WArgs slot.

### 7. dsl/Cte.scala / SetOpQuery.scala / Values.scala / CaseExpr.scala
Args plumbing for inner-fragment composition. CASE WHEN branches each carry their own Args; the result is `Concat` of all arms.

### 8. Remaining pg/functions/* (mechanical, batchable)
PgNumeric, PgTime, PgArray, PgGrouping, PgNull, PgRange, PgSession, PgSrf, PgSubquery, PgWindow + `pg/ArrayOps.scala` + `pg/RangeOps.scala`. Pattern is established (see `PgString` in commit `5c06bf4` and `PgAggregate` in `c8ad8dd`):
- TypedExpr inputs gain Args parameters; result Args = `Concat` of inputs.
- Runtime String/Int args bake via `Param.bind`, contributing `Void`.
- Variadic functions collapse to `TypedExpr[?, ?]` and existential result Args.
Do these in one batch commit after dsl/* is stable.

### 9. Tests
Migration patterns:
- `=== value` → keep working (value-taking overload routes through `Param.bind`, same SQL output) OR change to `=== Param[T]` for the static-template form.
- `.compile.run(session)` → `.compile.run(session)(args)` when Args is non-Void; bare `.run(session)` works for Args = Void.
- `.compile.af` → still works (delegates to fragment(args)) but only for Void-Args; otherwise call `.bind(args).af` if/when that helper lands.
- SQL-string assertions stay the same — same SQL output regardless of TypedExpr internals.

## Reference: design constraints captured in this conversation

- **`AppliedFragment` is no longer the universal currency**. Used only for: structural tokens (RawConstants), `whereRaw`/`havingRaw` payloads, the final Skunk-side `session.execute(query)(args)` call. All the DSL machinery now uses typed `Fragment[Args]`.
- **`Param[T]` is a `TypedExpr[T, T]`** — slots wherever any TypedExpr is accepted (WHERE, HAVING, ORDER BY, LIMIT/OFFSET, SET, INSERT VALUES, JOIN ON, function args, CASE branches, projections). No special overloads needed for Param-specific call sites.
- **`Param.bind[T](v): TypedExpr[T, Void]`** is the migration helper / dynamic-context bridge. Bakes a runtime value into a Void-args fragment via `pf.codec.contramap[Void](_ => v)`. Equivalent to today's captured-args path, just explicit. Most user code shouldn't need it; operators use it under the hood when given a value RHS.
- **`lit(v)` is for compile-time primitive constants only** (Int/Long/Bool/Float/Double/String/Short/Byte). String literals are safe (lifted from source code). Returns `TypedExpr[T, Void]`.

## Quick-start commands for the new session

```sh
cd /Users/ragb/dev/skunk-sharp
git checkout param-placeholders
git log --oneline -5            # confirm at 5c06bf4
cat PARAM_MIGRATION.md           # full plan
cat PARAM_MIGRATION_RESUME.md    # this doc
```

Start with `modules/core/src/main/scala/skunk/sharp/dsl/Compiled.scala`.
