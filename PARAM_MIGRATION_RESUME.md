# Resume: Param migration (TypedExpr[T] → TypedExpr[T, Args])

**Branch**: `macro-sql-assembly`

All green:

- `core` 488/488
- `circe` 10/10
- `iron` 4/4
- `refined` 5/5
- `tests` 159/159 (Postgres testcontainers)
- **total 666/666**

CompileBench (200,000 iterations × 5 scenarios — SELECT, INSERT, UPDATE, DELETE, JOIN) reports **0 dynamic AppliedFragments per compile**. See [`CompileBench.scala`](modules/core/src/test/scala/skunk/sharp/bench/CompileBench.scala).

For per-commit history see `git log --oneline macro-sql-assembly`. This file tracks the design summary and what's left.

## What's done

`Param[T]` works as a deferred-parameter typed template on every position that takes an expression. The captured-args type surfaces in `CompiledQuery[Args, R]` / `CompiledCommand[Args]`:

```scala
val byId: QueryTemplate[UUID, NamedRow] =
  users.select.where(u => u.id === Param[UUID]).compile

prep <- byId.prepared(session)
user <- prep.unique(realUuid)
```

Verified across the entire DSL surface (see [`ParamSuite.scala`](modules/core/src/test/scala/skunk/sharp/ParamSuite.scala) — 109 tests):

- **SELECT**: WHERE, HAVING (single + chained), GROUP BY (`Pg.groupingSets` / `cube` / `rollup` are exempt — they're set-spec functions, not value expressions), DISTINCT ON, ORDER BY, projection elements, `IN` value list, `BETWEEN` bounds, `LIKE`/`ILIKE`/`SIMILAR TO` patterns.
- **JOIN**: `.on` predicates (typed via `OnArgs` slot on `SourceEntry`), correlated LATERAL inner WHERE.
- **Subquery `.alias`** (single-source via `SelectBuilder.alias`, multi-source via `ProjectedSelect.alias`) — inner `WA`/`HA`/`GroupArgs`/`OnA` thread into the outer relation's `BodyArgs` and surface in the outer `Args` via `SourceBodyArgsProj`.
- **CTE bodies** — `cte("active", users.where(_.id === Param[UUID]))` — typed body args fold into the outer `CteArgs` slot. Re-aliasing via `.alias("x")` preserves `BodyArgs` (CteRelation has a fourth `Alias_` type parameter, decoupled from `Name`).
- **CASE WHEN** — branch conditions, `THEN` results, `ELSE` results all thread their args.
- **UPDATE**: `SET` (single + tuple + `&`-chained), WHERE, RETURNING (single + tuple + all). `UPDATE … FROM <typed-args subquery>` threads inner Param into outer `Args`.
- **DELETE**: WHERE, RETURNING (single + tuple + all). `DELETE … USING <typed-args subquery>` threads inner Param.
- **INSERT**: row-tuple `Args` for single-row, batched via `cats.Reducible`, `INSERT … FROM SELECT`, `RETURNING` variants. `ON CONFLICT DO UPDATE` threads `CA` (set-clause args) as a third `InsertCommand` type parameter.
- **SRF**: `Pg.generateSeries(Param[Int], Param[Int])`, `Pg.unnestAsRelation(Param[Arr[E]])`. Function args render as a typed `Right` slot via the `IsSrf` marker in `aliasedFromEntryParts`.
- **PgFunction operators**: typed Args through `Pg.lower`, `Pg.upper`, `Pg.length`, `Pg.lpad`/`rpad`, `Pg.concat`, `Pg.coalesce`, `Pg.makeDate`, `Pg.power`, `Pg.mod`, `Pg.greatest`, `Pg.least`, `Pg.overlaps`, `Pg.lag`, `Pg.stringAgg`, range/array operators, jsonb operators. Variadic functions (`coalesce` / `greatest` / `least` / `concat`) thread typed Args via [[Where.FoldConcatN]] up to arity 9.
- **Window `OVER (…)` specs**: `WindowSpec.partitionBy(Param)` and `WindowSpec.orderBy(Param.asc)` thread `(PA, OA)` typed slots into the wrapping expression's `Args` via `Concat[A, Concat[PA, OA]]`. Frame bounds (`rowsBetween` / `rangeBetween` / `groupsBetween`) are static integer constants — Args-neutral.
- **`SetOpQuery[A, R]`**: carries an `Args` type parameter; each `.union` / `.intersect` / `.except` step concatenates arms via `Concat[A1, A2]`. Param-bearing arms (e.g. `users.select.where(u => u.email === Param[String]).union(...)`) thread their typed Args end-to-end into the outer `CompiledQuery`.
- **`IN` / `ANY` / `ALL (subquery)`**: now thread the inner subquery's typed Args. `col.in(<subquery>)` returns `Where[Concat[A, RA]]` — Param in the inner subquery surfaces as `RA` on the outer query. Value-list `IN (NonEmptyList(...))` still produces `Args = Void` (values are Param.bind-baked).
- **Named-tuple multi-item RETURNING**: `users.delete.where(...).returningNamed(u => (id = u.id, p = Pg.power(u.age, Param[Double])))` projects to `NamedTuple[("id", "p"), (UUID, Double)]` and threads the named tuple's value-Args via [[FoldConcatN]]. Available on every mutation builder (`InsertCommand`, `UpdateReady`, `UpdateFromReady`, `DeleteReady`, `DeleteUsingReady`).

## Static-by-default operators

The value-RHS overload was removed from binary operators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `between`, `notBetween`, `betweenSymmetric`, `isDistinctFrom`, `isNotDistinctFrom`, `like`, `ilike`, `similarTo`, `notSimilarTo`) and from `:=` in UPDATE SET. The user picks one of three explicit forms at every value site:

- `Param[T]` — deferred to execute time, threads `T` into outer `Args`. Static SQL.
- `lit(v)` — compile-time literal (primitives only). Inline SQL (`'x'`, `42`), `Args = Void`.
- `Param.bind(v)` — bake a runtime value into a `Void`-args fragment. Encoder closure-captures `v`; rebuilt per `.compile`.

Tests use `lit(...)` for literal cases and `Param.bind(...)` for variable cases. The `compiletime.testing.typeCheckErrors` negative tests for "Param in CTE / .alias / .on predicate not allowed" were flipped to positive tests now that those positions thread typed Args.

## Static-SQL bake-out

Every standard query shape is fully cached at compile time:

- **Operator macro baking** — `===`, `<=`, `BETWEEN`, etc. with a `TypedColumn` LHS produce a single `Fragment[Args]` whose `parts` is compile-time-constant strings.
- **Structural-token intern table** ([`RawConstants`](modules/core/src/main/scala/skunk/sharp/internal/RawConstants.scala)) — process-wide-shared `AppliedFragment`s for SQL keywords, separators, parentheses, ORDER BY direction keywords (`ASC`, `DESC`, `NULLS FIRST`, `NULLS LAST`).
- **LIMIT/OFFSET integer cache** — `RawConstants.limitAf(n)` / `offsetAf(n)` cache the first 1024 integer values.
- **Projection list cache** — `Relation.starProjAf` (`"col1", "col2", …`) and `starProjFromAfOpt` (`"col1", … FROM "qualifiedName"`) are per-Relation `lazy val`s. The compile path always uses `starProjAf` (column names are preserved by `nullabilifyCols`, so RIGHT/LEFT/FULL JOIN nullabilification doesn't force a dynamic projection rebuild).
- **`lazy val` caching** on `TypedColumn.render`, `Table.columnsView`, `Table.deleteFromHeader`, `Table.updateSetHeader`.
- **Fully-static fast path in `assembleN`** — when every part contributes no typed parameters, the assembled `Fragment` uses the process-wide-shared `Void.codec` directly instead of a custom `Encoder`. Skips the per-execute parts walk.

## Pending

**Substrate cleanup** — three stacked simplifications, all sketched but not yet landed:

1. *Drop `Concat2` / `FoldConcatN` typeclasses.* The priority-chain givens are redundant: `inline def projectConcat[A, B](c: Concat[A, B]): (A, B)` with `erasedValue` dispatch produces the four shape branches (`Void/Void`, `Void/B`, `A/Void`, neither) with no typeclass machinery. Same for `projectFoldConcat[T <: Tuple]` via inline tuple recursion. Cost: every method that takes `using c2: Concat2[A, B]` (across all builders, operators, helpers) drops the using-clause and becomes `inline def`; calls to `c2.project(x)` become `Where.projectConcat[A, B](x)`. Inline propagation cascades — methods calling these become inline too. Mechanical sweep across ~25 files.

2. *Drop AF from `BodyPart`.* `Either[AppliedFragment, Fragment[?]]` → `Either[Fragment[?], Fragment[?]]`: both arms hold Fragments, the Left/Right tag is purely positional (Left = no slot in Args tuple; Right = takes a positional slot, even if its encoder is Void). `RawConstants` keywords keep their AF form for execute-boundary use; emit sites do `.fragment` to extract the typed Fragment. Walker in `assembleN` becomes uniform — no `case Left(af) => af.fragment.parts` vs `case Right(f) => f.parts` split. Subtle: Param.bind-baked Lefts have non-empty encoder types but take Void input — walker calls `f.encoder.encode(skunk.Void)`.

3. *Smart `Concat` with flat tuples.* Replace the existing `Concat[A, B]` (which produces nested `Tuple2`s for 3+ params: `((T1, T2), T3)`) with one that flattens via `Tuple.Concat[ToTuple[A], ToTuple[B]]` (where `ToTuple[X] = X` if `X <: Tuple`, else `X *: EmptyTuple`). Result: 1-param queries stay `Args = T` (single value preserved), 2-param `(T1, T2)`, 3-param flat `(T1, T2, T3)` — matches user intuition, simplifies encoder splitting via `tuple.take(aLen) ++ tuple.drop(aLen)`. UX impact: tests asserting `((T1, T2), T3)` shapes update to `(T1, T2, T3)`.

These three are independent and stackable. Recommended order: (1) first (largest mechanical sweep, removes the most code), (2) next (substrate uniformity), (3) last (UX win).

**Top-level builder-chain owner macro** — issue #24's acid test. A macro that resolves the entire builder chain so any `.compile` whose structure is compile-time-known (parts list of constant strings + encoder built from singleton codecs and `Param[T]`-supplied per-type codecs) collapses to a *single interned* `Fragment[Args]` constant at expansion time. `Args = Void` is the trivial subcase (no `Param`); the general case is `Fragment[A]` where `A` is the threaded captured-args tuple — same collapse, same allocation savings. Today each leaf macro-bakes its own `parts` list and they concatenate at runtime via shared-AF references in `assembleN` — the AFs are reused but the parts list is rebuilt per compile. Substantial Scala 3 macro project. The accompanying compile-time assertion (positive: static-shape queries ARE constants; negative: dynamic-shape queries do NOT collapse) depends on this — easiest to express for the `Args = Void` subcase (`Void.codec` reference-equality), more involved for the typed case (encoder is a product of singleton codecs).

Best done *after* the substrate cleanup — uniform Fragment-only `BodyPart` and inline-everywhere dispatch make the macro target much cleaner.

**Small typed-Args holdouts** — none. Every Args-loss position is closed; `Param` surfaces in the outer `Args` everywhere it parses.

## Roadmap

Outside the typed-Args migration but tracked alongside (see CLAUDE.md):

- Companion modules: `skunk-sharp-refined`, `skunk-sharp-ltree`, `skunk-sharp-fts`, `skunk-sharp-postgis`.
- Docs site via `sbt-typelevel-site` + mdoc.
