# Resume: Param migration (TypedExpr[T] → TypedExpr[T, Args])

**Branch**: `macro-sql-assembly`
**Head**: `e3b0d58` — pushed to remote.

| module    | tests   | status |
| --------- | ------- | ------ |
| core      | 439/439 | ✅     |
| circe     | 10/10   | ✅     |
| iron      | 4/4     | ✅     |
| refined   | 5/5     | ✅     |
| tests     | 159/159 | ✅ (Postgres testcontainers) |
| **total** | **617/617** | ✅ |

## Latest session (commits `90160ed` → `e3b0d58`)

- `e3b0d58` — **Round out variadic-typed builders**:
  - `Pg.lpad(e, n, fill)` / `Pg.rpad(e, n, fill)`: `n` and `fill`
    baked; Args propagates from `e` only.
  - `Pg.lag(expr, offset, default)` / `Pg.lead(...)`: same shape —
    `offset` and `default` baked, Args from `expr`.
  - `rangeCtor3` (and the public `int4range` / `int8range` /
    `numrange` / `daterange` / `tsrange` / `tstzrange` with `bounds:
    String`): `bounds` baked; Args = `Concat[X, Y]` from `lo` / `hi`.
  - `jsonbSet(target, path, value, createIfMissing)` and
    `jsonbInsert(target, path, value, insertAfter)`: `path` and flag
    baked; Args = `Concat[X, Y]` from `target` / `value`.
  - `Pg.makeTimestamp(year, month, day, h, m, s)`: 6 typed positions
    threaded as left-fold Concat (5 levels of Concat2 evidence).

- `01d5f02` — **More variadic-typed**: `Pg.greatest` / `Pg.least`
  (arity 1/2/3 + Void fallback), `Pg.makeDate` / `Pg.makeTime` (3
  fixed positions threaded as `Concat[Concat[A, B], C]`), and
  `Pg.overlaps` (4 fixed positions, hand-rolled encoder for the
  `(a, b) OVERLAPS (c, d)` dual-pair separator pattern).
  `Pg.makeTimestamp` (6 positions) kept at `Args = Void` until
  needed — would need a generic combineList variant with custom seps
  or a 6-Concat boilerplate. Same pattern can be ported to
  `Pg.lpad/rpad` (with fill), `Pg.lag/lead` (with default), `CASE
  WHEN`, jsonb mutators (`jsonbSet`, `jsonbInsert`), `rangeCtor3`.

- `c154ee9` — **Variadic-typed `Pg.coalesce` / `Pg.concat`** thread
  typed Args at arity 1, 2, 3; variadic fallback at `Args = Void` for
  N > 3. Combined Args is left-folded `Concat` to match assembleN.

  ```scala
  users.select(u => Pg.coalesce(Param[String], u.email, Param[String]))
       .where(u => u.id === Param[UUID])
       .compile
    // : QueryTemplate[((String, String), UUID), String]
  ```

- `ea30376` — **ORDER BY threads typed `OArgs`** (6-slot
  `ProjectedSelect.compile`). `OrderBy[A]` is now parametric — `.asc /
  .desc / .nullsFirst / .nullsLast` preserve A, so a Param-bearing
  order key (`Pg.mod(u.age, Param[Int]).desc`) carries `Int` through to
  the final QueryTemplate Args slot. ProjectedSelect adds `Orders <:
  Tuple` (default `EmptyTuple`); `.orderBy(...)` is `transparent inline`
  extending Orders via `Tuple.Concat[Orders, NormProj[O]]`. compile
  takes `[DArgs, ProjArgs, GArgs, OArgs]`, summons `ProjArgsOf` for all
  four of `Proj` / `Groups` / `DistinctOn` / `Orders`, and routes
  through `assemble6`.

  Render order: `[DIST=0, PROJ=1, WHERE=2, GROUP=3, HAVING=4,
  ORDER=5]`. Final Args:
  `Concat[Concat[Concat[Concat[Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs], OArgs]`.

  ```scala
  users.select(u => Pg.power(u.age, Param[Double]))
       .distinctOn(u => Pg.mod(u.age, Param[Int]))
       .where(u => u.id === Param[UUID])
       .groupBy(u => Pg.mod(u.age, Param[Int]))
       .orderBy(u => Pg.mod(u.age, Param[Int]).asc)
       .compile
    // : QueryTemplate[((((Int, Double), UUID), Int), Int), Double]
  ```

  ParamSuite: 4 new tests covering column-ref orderBy collapse,
  Param-bearing orderBy threading, full 6-slot composition, encoder
  render-order check.

- `2015e69` — **DISTINCT ON threads typed `DArgs`** (5-slot
  `ProjectedSelect.compile`). `ProjectedSelect` carries a new
  `DistinctOn <: Tuple` class type param (default `EmptyTuple`);
  `.distinctOn(...)` is `transparent inline` extending DistinctOn via
  `NormProj[D]`. compile takes `[DArgs, ProjArgs, GArgs]` method
  type params, summons `ProjArgsOf` for all three of `Proj` /
  `Groups` / `DistinctOn`, and routes through a new `assemble5` that
  dispatches Right slots positionally to `(a1..a5)`.

  Render order: `[DIST=0, PROJ=1, WHERE=2, GROUP=3, HAVING=4]`. Each
  typed slot is always emitted as a Right; absent clauses use
  `emptyVoidSlot` placeholders. The DISTINCT ON keyword + close-paren
  are emitted only when distinctOnOpt is set; otherwise the SELECT
  prefix collapses to plain `SELECT` / `SELECT DISTINCT`. Final Args:
  `Concat[Concat[Concat[Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs]`.

  ```scala
  users.select(u => Pg.power(u.age, Param[Double]))
       .distinctOn(u => Pg.mod(u.age, Param[Int]))
       .where(u => u.id === Param[UUID])
       .groupBy(u => Pg.mod(u.age, Param[Int]))
       .compile
    // : QueryTemplate[(((Int, Double), UUID), Int), Double]
  ```

  ParamSuite: 4 new tests (column-ref distinctOn collapses, Param
  threading, full 5-slot composition, encoder render-order check).

- `7a8c61c` — **GROUP BY threads typed `GArgs`** in
  `ProjectedSelect.compile`. Param-bearing items in GROUP BY now thread
  their `Args` into the QueryTemplate Args slot alongside the existing
  PROJ / WHERE / HAVING positions. Final Args:
  `Concat[Concat[Concat[ProjArgs, WArgs], GArgs], HArgs]`.

  ```scala
  users.select(u => Pg.power(u.age, Param[Double]))
       .where(u => u.id === Param[UUID])
       .groupBy(u => Pg.mod(u.age, Param[Int]))
       .compile
    // : QueryTemplate[((Double, UUID), Int), Double]
  ```

  Implementation: new `assemble4[A1, A2, A3, A4, R]` (same pattern as
  `assemble3` plus one more slot); `ProjectedSelect.compile` now takes
  `[ProjArgs, GArgs]` as method type params and summons `ProjArgsOf`
  for both `Proj` and `Groups`. `compileBodyParts` emits stable slot
  indices `[PROJ=0, WHERE=1, GROUP=2, HAVING=3]`, with
  `emptyVoidSlot` placeholders when WHERE / GROUP / HAVING are absent
  so the slot positions stay aligned.

  Runtime fallback for the `SelectBuilder.groupBy → .select` path:
  ProjectedSelect's static `Groups` is `EmptyTuple` while runtime
  `groupBys` carries items, so the typeclass projector returns `Nil`;
  size-check and fall back to a Void-fill so the slot's items each get
  Void (encoder skips them anyway). Same fallback for the projection
  list. ParamSuite: 4 new tests covering single-Param GROUP BY,
  GROUP BY collapse, full 4-slot composition, encoder round-trip.

- `a33b068` — **`assemble3` walker fix: always-increment typedIdx; thread
  evidences explicitly.** The previous walker counted only non-Void Right
  slots, which silently dropped the third value when an intermediate slot
  had a Void encoder (e.g. UPDATE SET=value-baked + WHERE Param +
  RETURNING Param dispatched RETURNING's value to a2 instead of a3).
  Worked around in earlier shapes by routing DELETE/INSERT through
  `withReturningTyped2` (2-slot); now fixed at the root, which is the
  prerequisite for adding a 4th typed slot (GROUP BY / ORDER BY /
  DISTINCT ON typed Args). Walker now treats every Right slot as a
  positional slot regardless of encoder Void-ness; project the user's
  Args into (a1, a2, a3) once via the supplied Concat2 evidences and
  dispatch by slot index.

  Evidence threading is the ergonomic catch: at abstract type parameters,
  Scala summons `default` over an in-scope param, so explicit
  `(using ...)` threading is needed across the chain. Updated:
  - `assemble` hand-builds rightVoid for c123 and passes c2 + c123 down
    to assemble3 explicitly.
  - `SelectBuilder.compile`, `MutationAssembly.command/withReturningTyped/
    withReturningTyped2`, `ProjectedSelect.compile` all pass evidences
    explicitly to their assemble callee.
  - `.alias` extension on `SelectBuilder` takes `c2` in its using clause
    and threads it through the renderInner closure (the closure was
    capturing `default` and crashing at execute time).
  - `AsSubquery.fromSelectBuilder` takes `c2` similarly.

- `99d0b6f` — **SELECT projections thread typed `ProjArgs`**.
  `ProjectedSelect.compile` now requires a `ProjArgsOf.Aux[Proj, ProjArgs]`
  evidence; Param-bearing projection items thread their `Args` into the
  final `QueryTemplate` Args slot ahead of WHERE and HAVING. The blocker
  (Scala 3.8 NamedTuple-vs-Tuple match-type disjointness) is dodged by
  rewriting `select` (table-bound + `empty.select`) as a
  `transparent inline def` that dispatches on the projection shape via
  `compiletime.erasedValue`:
  - **single TypedExpr** — `Proj = X *: EmptyTuple`, `Row = X`'s value type.
  - **named tuple**     — `Proj = NamedTuple.DropNames[X]`,
    `Row = NamedTuple[Names[X], ExprOutputs[Drop[X]]]`.
  - **plain tuple**     — `Proj = X & Tuple`, `Row = ExprOutputs[X]`.

  Each branch sets `Proj` to a concrete type, so `ProjArgsOf[Proj]`
  summons cleanly at the user's `.compile` call site (regardless of
  whether X was a named tuple).

  ```scala
  users.select(u => (u.id, Pg.power(u.age, Param[Double])))
       .where(u => u.id === Param[UUID])
       .compile
    // : QueryTemplate[((Double, UUID)), (UUID, Double)]
  ```

  ParamSuite: 8 new tests — single TypedExpr / plain tuple / named tuple
  projections (with and without Param), encoder round-trip on the
  `Pg.power(Param) + WHERE Param` mixed shape.

- `56a3d28` — Add `ProjArgsOf` typeclass: foundation infrastructure for
  the above. Contravariant in T (so `TypedColumn` resolves via the
  `TypedExpr` leaf given). Priority chain: `NamedTuple` → `EmptyTuple` →
  `H *: EmptyTuple` (single-item) → multi-item cons → `TypedExpr` leaf.

- `90160ed` — **Multi-item RETURNING threads typed `RetArgs`** via the new
  `TypedExpr.combineList[Combined]` primitive + `Where.FoldConcat[T]` /
  `FoldConcatN[T]` typeclasses + `CollectArgs[T]` match type. The same
  primitive is the building block for the remaining roadmap positions
  (SELECT projections, GROUP BY/ORDER BY/DISTINCT ON, variadic builders).
  - `returningTuple` on Update / Delete / Insert / DeleteUsing /
    UpdateFromReady combines items into a single typed `Fragment`,
    threading `FoldConcat[CollectArgs[T]]` as the RetArgs slot.
  - `returningAll` builds a Void-Args combined fragment of column refs.
  - New `MutationAssembly.withReturningTyped2[A1, RetArgs, R]` (DELETE /
    INSERT, two-slot: WHERE/VALUES + RETURNING). Splits cleanly from
    UPDATE's three-slot `withReturningTyped` so the slot positions in
    `assemble3` align with the (A1, A2, A3) type params — the prior
    DELETE+RETURNING path had a latent walker bug at typedCount=2.
  - 8 new ParamSuite tests covering Param-bearing multi-item RETURNING
    (encoder round-trips and static type assertions).
- `46bd9fb` — **Variadic builder return types** (`Pg.greatest`,
  `Pg.least`, `Pg.coalesce`, `Pg.concat`) tightened from `TypedExpr[T, ?]`
  to `TypedExpr[T, Void]`. Matches their actual runtime shape (collapsed
  via `joinedVoid`) and lets them slot cleanly into `CollectArgs[T]`
  reductions used by RETURNING. Variadic typed-Args for these is roadmap
  (needs arity-overload + tuple-input redesign).

**Combined-list runtime auto-projector experiment (reverted):** tried
replacing `FoldConcatN.project` with a runtime auto-projector inside
`combineList` that walks encoder Void-ness to peel values from `Combined`.
Functionally correct for runtime dispatch and would have eliminated the
typeclass evidence requirement. Reverted because the static
type-reduction issue (named-tuple `NormProj`) was orthogonal to the
auto-projector — keeping `FoldConcatN` preserves the explicit, type-
driven path that's easier to reason about. The auto-projector remains a
viable design choice if/when the named-tuple reduction is solved by
other means.

## Post-merge follow-up work (commits `fb2fe1b` → `e7c2f87`)

- `fb2fe1b` — threaded `Concat2` through `PgFunction.binary`, `PgOperator.infix`,
  `ArrayOps`, `RangeOps` so third-party operators built on these factories
  propagate typed Args correctly.
- `21d25b9` — **UPDATE SET supports `Param[T]`**. `MutationAssembly.command` now
  takes `[A1, A2]`. `UpdateBuilder.set` splits into single-SetAssignment (typed)
  and tuple-form (Void) overloads, `setFragment` routes via `Right(typed)` in
  `updateParts`. The known-broken `Fragment[Void].apply(Void)` cast is gone.
- `ad10e67` — `ParamSuite` covers `Pg.mod` / `Pg.power` / range / array binary ops
  with Param in WHERE position; encoder round-trip verified.
- `7c92ffc` — **single-expression RETURNING threads `RetArgs`**. New
  `SelectBuilder.assemble3[A1, A2, A3, R]` handles up to three typed slots in
  render order. `MutationAssembly.withReturningTyped` routes a single returning
  Fragment via `Right(typed)`. `.returning(f)` on Update/Delete/Insert returns
  `QueryTemplate[Concat[Concat[A1,A2], A], T]`.
- `e7c2f87` — example repositories ported to static `QueryTemplate` style. Both
  `RoomRepository` and `BookingRepository` compile each fixed-shape query once at
  object construction. `RoomRepository.patch` is the lone remaining captured-args
  call — a documented limitation, not a missing feature.

## Open gaps (priority order)

1. **`SelectBuilder.groupBy` doesn't track `Groups` type-level**.
   When the user does `SelectBuilder.groupBy → .select`, the
   ProjectedSelect inherits the runtime `groupBys` list but `Groups`
   resets to `EmptyTuple`. Compile() handles this with a runtime size
   fallback (Void-fill); typed Args from such a path don't surface.
   Refactor: add `Groups` to `SelectBuilder`'s class type params (38
   refs); make `SelectBuilder.groupBy` transparent-inline so the type
   threads through `.select`.
2. **CASE WHEN typed Args**. Branches' Args are widened to `?` today.
   Threading needs `CaseWhen[T, A]` parametric carrying the running
   Concat-fold of branch + ELSE arms; `caseWhen` / `.when` /
   `.otherwise` rewritten to extend A. The runtime walker pattern is
   the same as combineList but the public API needs the type-level
   tracking.
3. **`UPDATE … FROM` / `DELETE … USING` typed Args from the USING/FROM
   source**. Currently the inner relation is bound at Void.
4. **Subquery `.alias` / CTE bodies with typed inner Args**.
   `SelectBuilder.alias` and `compileFragment` bind `Void`; threading
   inner subquery's Args through `.alias` / CTE is roadmap.

The unifying engineering work is the now-shipped helper
`TypedExpr.combineList[Combined](items, sep, projector): Fragment[Combined]`
plus the static fold-Concat machinery (`Where.FoldConcat[T]`,
`Where.FoldConcatN[T]`, `CollectArgs[T]`) AND the `ProjArgsOf` typeclass
(for shapes that can't reduce as match types — named tuples). Done for
RETURNING and SELECT projections; reuse for the remaining positions.

## Headline result

`Param[T]` works as a typed-template placeholder on every static-query position that matters:

```scala
val byId: QueryTemplate[UUID, NamedRow] =
  users.select.where(u => u.id === Param[UUID]).compile

prep <- byId.prepared(session)
user <- prep.unique(realUuid)
```

Verified on SELECT WHERE / HAVING (single + chained), JOINed SELECT WHERE,
UPDATE WHERE, DELETE WHERE, DELETE … RETURNING, and `INSERT.withParams` — see
`modules/core/src/test/scala/skunk/sharp/ParamSuite.scala`.

## Key design pieces

- **`Where.Concat2[A, B]`** — typeclass with `bothVoid` / `leftVoid` / `rightVoid` /
  `default` instances. Generic version of skunk's `Fragment.~> / <~`. Threaded
  through `combineSep` / `combine` / `opCombine` / `between` / `quantifiedRender`
  so summon happens where A and B are concrete and the right contramap is picked.
- **`assemble` rewrite** — custom `Encoder[Concat[A1, A2]]` that walks the body-parts
  list at encode time, dispatching `Left(af)` to its baked argument and `Right(f)` to
  the user's typed Args. Preserves SQL render order in the encoded value list, which
  matters because Postgres binds positionally.
- **`TypedExpr.joinedVoid`** — variadic helper for `Pg.concat`, `Pg.coalesce`,
  `Pg.makeDate`, `Pg.overlaps`, `Pg.lag(default)`, `Pg.lpad/rpad(fill)`,
  `PgJsonb.jsonbSet/jsonbInsert`, `PgRange.rangeCtor3`, CASE WHEN — anywhere you
  combine N fragments at Void-Void in one shot.
- **`InsertCommand.withParams`** — typed-Args INSERT. `(id = Param[UUID], email =
  Param[String], age = Param[Int])` → `CommandTemplate[(UUID, String, Int)]` via the
  `StripParams` match type that unwraps each `Param[T]` to `T`.

## Known limitations (carried forward from migration design)

- **`Stripped[T]` not used in operator overloads.** Match types in overloaded
  extension parameter positions break Scala 3 overload resolution. Trade-off:
  nullable-column value-RHS comparisons need `Some(value)` / `None` (rare in
  practice; `.isNull` / `.isNotNull` is the right tool for SQL NULL semantics).
- **UPDATE SET RHS `:= Param[T]` is unsafe.** `.set` forces SetArgs = Void and
  the encoder still expects T at execute. Use baked values (`:= "x"`) for SET.
  Re-add typed Param[T] in SET when per-row Args reduction lands.
- **CTE bodies / SetOp / Values / `.alias` subqueries / window OVER specs / `select`
  projections / ORDER BY exprs / DISTINCT ON / RETURNING items / ON CONFLICT DO
  UPDATE / ON predicates** — Args bound at Void via `bindVoid` at materialisation.
  Encoder still threads bound values correctly (proven by integration tests passing),
  but the user-visible `Args` doesn't surface typed Params from these positions.
  Per-position typed-Args is roadmap.
- **Variadic CASE WHEN / Pg.overlaps / Pg.makeDate / Pg.greatest / Pg.least /
  Pg.coalesce / Pg.concat / etc.** all collapse Args to `Void`. Mixing typed
  `Param[T]` inside variadic builders works at runtime (encoder still emits)
  but the result's typed Args slot is `Void`.
- **Multi-item RETURNING with named tuples** is unsupported — only plain
  tuple projections (`returningTuple(u => (u.id, u.email))`) thread typed
  RetArgs. Named tuples in the RETURNING projection lambda would hit the
  Scala 3.8 NamedTuple-vs-Tuple match-type blocker. (SELECT projections
  use `erasedValue` dispatch to dodge this — the same trick can be ported
  to RETURNING when needed.)
**Plan**: see [`PARAM_MIGRATION.md`](PARAM_MIGRATION.md) for the full file-by-file checklist + design rationale.

## What's done (6 commits on the branch)

1. `e1de635` — migration plan doc
2. `9896df4` — `TypedExpr[T, Args]` (typed `Fragment[Args]`, not AppliedFragment), `TypedColumn` extends `TypedExpr[T, Void]`, `Param[T] extends TypedExpr[T, T]`, `type Where[A] = TypedExpr[Boolean, A]`
3. `6360d91` — `litMacro` returns `TypedExpr[T, Void]`, `ops/ExprOps.scala` rewritten with Args-threading + `Param.bind` shortcuts, **`internal/SqlMacros.scala` deleted**
4. `c8ad8dd` — `PgFunction.{nullary,unary,binary,nary}` and `PgOperator.{infix,prefix,postfix}` thread Args, `pg/functions/Shared` helpers updated, `PgAggregate` migrated
5. `5c06bf4` — `PgString` migrated
6. **NEW**: `dsl/Compiled.scala` rewritten — `CompiledQuery` → `QueryTemplate`, `CompiledCommand` → `CommandTemplate`, args dropped from struct, execution methods take args at execute time (`q.run(s)(args)`), `Args = Void` overloads (`q.run(s)`), `AsSubquery[Q, T, Args]` (3-param) threading inner subquery args into outer composition, `.asExpr` returns `TypedExpr[T, Args]`. Bridge helper `QueryTemplate.fromApplied` and `CommandTemplate.fromApplied` for code paths still emitting `AppliedFragment` (SetOpQuery, Values).

## What's pending (next session — resume in this order)

The user explicitly chose to pause pg/functions/* (which is mechanical "thread A through") and **resume with the dsl/* layer first** — that's where the user-visible API decisions surface.

### 1. dsl/Compiled.scala — DONE in commit `[next]`

- `QueryTemplate[Args, R]` / `CommandTemplate[Args]` carry only `fragment` + `codec` (no captured args).
- Execution extensions take `args: Args` at execute time. `Args = Void` overloads provide argless `q.run(session)` / `c.run(session)`.
- `q.bind(args): AppliedFragment` for explicit pre-application; `q.af` available only on the `Args = Void` overload.
- `AsSubquery[Q, T, Args]` is the new 3-param trait. Instances: `identity`, `fromProjected`, `fromSelectBuilder`, `fromSetOp` (Args = Void via `liftAfToVoid`), `fromValues` (Args = Void via `liftAfToVoid`).
- `q.asExpr[T, Args]` returns `TypedExpr[T, Args]` — inner subquery args thread into the outer expression.
- `QueryTemplate.fromApplied` / `CommandTemplate.fromApplied` are bridge helpers for code paths still emitting `AppliedFragment` (SetOpQuery, Values).

### 1b. Small mechanical files — DONE in commit `[next+1]`

- `dsl/ProjCols.scala` — `AliasedExpr[t, n]` → `AliasedExpr[t, n, ?]`. `consAliased` given gains an `A` parameter.
- `dsl/package.scala` — `TypedExpr[T]` alias → `TypedExpr[T, Args]`. `AliasedExpr` aliased with 3 params. Re-exports `Param`. `lit` / `param` return `TypedExpr[T, Void]`. `caseWhen` lambda generic over branches' Args.
- `dsl/CaseExpr.scala` — `CaseWhen[T]` carries `(TypedExpr[Boolean, ?], TypedExpr[T, ?])` branches. `.when` / `.otherwise` accept any branch Args. The combined CASE expression's Args is widened to `?` (typed-args threading through CASE branches is roadmap). `renderCaseWhen` rebuilt to walk branches and concat parts + product encoders directly (no `.render`).
- `dsl/SetOpQuery.scala` — `.compile` returns `QueryTemplate[Void, R]` via `QueryTemplate.fromApplied`. `.union` / `.unionAll` / `.intersect` / `.intersectAll` / `.except` / `.exceptAll` accept `AsSubquery[Q, R, A]` (3-param). The internal `append` materialises the right via `ev.fragment(right)` and binds `Void` to produce an `AppliedFragment` for the closure chain. **Constraint**: works when right's `Args = Void`; broader typed-args threading requires `SetOpQuery` itself to carry an Args parameter (roadmap).

### 1c. dsl block: Select / Update / Delete / Insert / Join — DONE in commit `[next+2]`

- **Select.scala** — `BodyPart` simplified to `Fragment[?]` (uniform, no Left/Right). `whereOpt` / `havingOpt` are `Option[Fragment[?]]` (dropped Any half). `groupBys` / `distinctOnOpt` / `projections` are `List[TypedExpr[?, ?]]`. `OrderBy(fragment: Fragment[?])` carries typed Args (so `Param[Int].desc` threads). `andInto` / `andRawInto` produce `Fragment[?]`. `bodyPartsAround` builds the body as `List[Fragment[?]]` via `liftAfToVoid` for structural pieces. `assemble[Args, R]` walks parts and accumulates encoder via Void-aware product, returning `QueryTemplate[Args, R]`. `compile` returns `QueryTemplate[Where.Concat[WArgs, HArgs], Row]`. `compileFragment` retained as bridge for Cte / `.alias` paths — binds `Void` (constrained to Args=Void inner queries; typed-args threading roadmap). `renderClauses` removed.
- **Update.scala** — `SetAssignment[T, Args]` no longer carries `args`. `:=(value: T)` routes via `Param.bind` → `SetAssignment[T, Void]`; `:=[A](expr: TypedExpr[T, A])` typed. `combineAll` returns `Fragment[?]`. `whereOpt: Option[Fragment[?]]`. `compile` returns `CommandTemplate[Where.Concat[SetArgs, WArgs]]`. `returning*` returns `QueryTemplate[..., R]`. Same shape on `UpdateFromBuilder` / `UpdateFromReady`.
- **Delete.scala** — `whereOpt: Option[Fragment[?]]`. `compile` returns `CommandTemplate[Args]`, `returning*` returns `QueryTemplate[Args, R]`. `MutationAssembly.command` / `withReturning` route through `SelectBuilder.assemble`.
- **Insert.scala** — Existing whole-row / batch / from-query call surface preserved. Single-row and batch values bake via `Param.bind` so `Args = Void` (was the row tuple before). `INSERT … SELECT` threads inner subquery's `Args` via 3-param `AsSubquery`. **Decision**: dropped the `into / values / valuesOf` projection-then-values design. With per-field Args reduction not yet shipped, `.values(typedExprs)` could only return `Args = ?`, defeating the typed-template purpose. `.valuesOf` would have duplicated the existing `users.insert(rowNT)` shortcut. Roadmap: per-field Args reduction typeclass, then re-add the projection flow.
- **Join.scala** — Minor: `buildProjectedCols` takes `List[TypedExpr[?, ?]]`; `AliasedExpr[?, ?, ?]` (3-param) match. `IncompleteJoin.on[A]` accepts `OnView => TypedExpr[Boolean, A]`. `SelectBuilder.alias` materialises inner via `compile.fragment.bind(Void)` (constrained to Args=Void inner — typed-args through `.alias` is roadmap).

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
