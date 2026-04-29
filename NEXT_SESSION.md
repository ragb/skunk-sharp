# Next session: thread typed Args through multi-item / variadic positions

**Branch**: `macro-sql-assembly` (already pushed; HEAD `79ad34f`)
**Starting state**: 575/575 tests passing, single-expression Param coverage complete (see [`PARAM_MIGRATION_RESUME.md`](PARAM_MIGRATION_RESUME.md)).

## The unifying chunk

Four roadmap items collapse into one piece of engineering:

1. Multi-item `returningTuple` / `returningAll` typed Args
2. SELECT projection-position Param
3. GROUP BY / ORDER BY / DISTINCT ON Param
4. Variadic builders (`Pg.coalesce`, `concat`, `Pg.overlaps`, `CASE WHEN`, `rangeCtor3`, `Pg.makeDate`, …)

All four reduce to: **combine N typed `Fragment[?]` items into one `Fragment[Combined]` whose `Combined` is the fold-Concat of the individual Args, with a separator between items.**

Ship this primitive once, then plug it into all four positions.

## Concrete plan

### Step 1 — `combineList` typed combiner (90 minutes)

Add to [`TypedExpr.scala`](modules/core/src/main/scala/skunk/sharp/TypedExpr.scala):

```scala
/**
 * Combine N typed Fragments into one whose Args is the caller-asserted `Combined` shape. The
 * Combined value is projected to a List[Any] of per-item args at encode time via the supplied
 * projector. Used by variadic builders / projection lists / RETURNING tuples.
 *
 * The caller is responsible for matching `Combined` to the actual fold-Concat of item Args —
 * this is enforced at the call site by a fold of `Where.Concat2` typeclass instances.
 */
private[sharp] def combineList[Combined](
  items: List[Fragment[?]],
  sep: String,
  projector: Combined => List[Any]
): Fragment[Combined]
```

The encoder walks `items` in render order, dispatching each item's encoder against the corresponding entry in `projector(args)`.

### Step 2 — Fold-Concat match type + typeclass (60 minutes)

In [`Where.scala`](modules/core/src/main/scala/skunk/sharp/where/Where.scala):

```scala
/** Fold-Concat over a tuple of Args types. Drops Void slots cleanly. */
type FoldConcat[T <: Tuple] = T match {
  case EmptyTuple        => Void
  case h *: EmptyTuple   => h
  case h *: t            => Concat[h, FoldConcat[t]]
}

/** Project a fold-Concat value back into the heterogeneous list of per-item args. */
trait FoldConcatN[T <: Tuple] {
  def project(c: FoldConcat[T]): List[Any]
}
object FoldConcatN {
  given empty: FoldConcatN[EmptyTuple] = ...
  given single[H]: FoldConcatN[H *: EmptyTuple] = ...
  given cons[H, T <: NonEmptyTuple](using
    c2: Concat2[H, FoldConcat[T]],
    rest: FoldConcatN[T]
  ): FoldConcatN[H *: T] = ...
}
```

Match type for extracting Args from a tuple of TypedExprs:

```scala
type CollectArgs[T <: Tuple] <: Tuple = T match {
  case TypedExpr[?, a] *: tail => a *: CollectArgs[tail]
  case EmptyTuple              => EmptyTuple
}
```

### Step 3 — Apply to multi-item RETURNING (45 minutes)

In [`Delete.scala`](modules/core/src/main/scala/skunk/sharp/dsl/Delete.scala):

```scala
private[dsl] def withReturningTyped[A1, A2, RetArgs <: Tuple, R](
  base: List[BodyPart],
  returningExprs: List[TypedExpr[?, ?]],
  codec: Codec[R]
)(using
  c12:  Where.Concat2[A1, A2],
  cN:   FoldConcatN[RetArgs],
  c123: Where.Concat2[Where.Concat[A1, A2], FoldConcat[RetArgs]]
): QueryTemplate[Where.Concat[Where.Concat[A1, A2], FoldConcat[RetArgs]], R] = {
  val combined: Fragment[FoldConcat[RetArgs]] =
    TypedExpr.combineList(returningExprs.map(_.fragment), ", ", cN.project)
  val parts = base ++ List[BodyPart](Left(RawConstants.RETURNING), Right(combined))
  SelectBuilder.assemble3[A1, A2, FoldConcat[RetArgs], R](parts, Nil, codec)
}
```

Update `.returningTuple` on Update/Delete/Insert to thread `CollectArgs[T]` as the RetArgs tuple.

### Step 4 — Apply to SELECT projections (90 minutes)

Add a 7th type param `ProjArgs` to `ProjectedSelect`. Compute it via a transparent inline macro from the projection lambda's return type. The compile path becomes:

```scala
SelectBuilder.assemble3[ProjArgs, WArgs, HArgs, Row](...)
```

(Note: render order is proj → where → having, so `assemble3`'s slot indices line up directly.)

### Step 5 — Apply to GROUP BY / ORDER BY / DISTINCT ON (60 minutes)

Same pattern: thread `GroupArgs` / `OrderArgs` as additional type params. `compileBodyParts` routes them via `Right(typed)` instead of `bindVoid`.

This is where it gets tight: SELECT can have up to **5 typed slots** in render order (proj, where, group, having, order). `assemble3` caps at 3.

**Decision point**: either bump to `assemble5` (mechanical extension), or generalise to `assembleN` with a list of slot args. `assembleN` is the cleaner long-term solution — the existing `assemble3` already does render-order index dispatch, just generalise to a `List[Any]` of slot args + a runtime-walked Concat-fold.

### Step 6 — Variadic builders (60 minutes)

Replace `joinedVoid` with `combineList` in:
- [`PgFunction.scala`](modules/core/src/main/scala/skunk/sharp/PgFunction.scala): `nary`
- [`PgString.scala`](modules/core/src/main/scala/skunk/sharp/pg/functions/PgString.scala): `lpad`/`rpad` with fill
- [`PgWindow.scala`](modules/core/src/main/scala/skunk/sharp/pg/functions/PgWindow.scala): `lag`/`lead` with default
- [`PgRange.scala`](modules/core/src/main/scala/skunk/sharp/pg/functions/PgRange.scala): `rangeCtor3`
- [`PgTime.scala`](modules/core/src/main/scala/skunk/sharp/pg/functions/PgTime.scala): `Pg.overlaps`, `Pg.makeDate`, etc.
- [`CaseExpr.scala`](modules/core/src/main/scala/skunk/sharp/dsl/CaseExpr.scala): `caseWhen`
- [`circe/PgJsonb.scala`](modules/circe/src/main/scala/skunk/sharp/circe/PgJsonb.scala): `jsonbSet`/`jsonbInsert`

Each call site gains a `(using FoldConcatN[Args])` and changes its return type to use `FoldConcat[Args]` instead of `Void`.

## Tests to add

Per-position, mirror the `ParamSuite` patterns already in place:

- Static type assertion (e.g. `val _: QueryTemplate[(Int, String, UUID), R] = q`)
- SQL contains expected text
- Encoder round-trip via `q.bind(args)` checking encoded values match render order

Plus integration tests for the most common patterns:
- `RETURNING (id, expr_with_param)` round-trip
- `SELECT col, expr_with_param FROM …`
- `GROUP BY date_trunc('month', col)` (no Param, just verify nothing breaks)

## Open design questions to resolve up front

1. **`assembleN` vs `assemble5`**. The cleaner answer is `assembleN`, but it's harder to type — the slot args become `List[Any]` at runtime with a static `Combined` type. `assemble5` is mechanically easy and covers all current positions. Recommend: ship `assemble5` as a stepping stone, refactor to `assembleN` if a 6th position emerges.

2. **`ProjectedSelect` 7th type param**. Adding `ProjArgs` is a breaking-source change for anyone parameterising over `ProjectedSelect`. Acceptable since the project is pre-1.0. Confirm via `git grep ProjectedSelect\\[` that no third-party code in the repo depends on the arity.

3. **Variadic API at the call site**. Today `Pg.coalesce(args*)` is variadic. With typed Args, the user passes a tuple: `Pg.coalesce((a, b, c))`. OR: arity-overloaded entry points (`coalesce(a, b)`, `coalesce(a, b, c)`, …) for ergonomic reasons. Recommend: arity overloads up to N=5, variadic fallback at Args=Void for N>5.

## Success criteria

- All 575 existing tests still pass.
- New per-position Param tests added (~15-20 tests).
- The `RoomRepository.patch` and the multi-item RETURNING limitation notes can be removed from the codebase / resume note.
- `users.select(u => (u.id, Pg.power(u.age, Param[Double]))).compile` produces `QueryTemplate[Double, (UUID, Double)]`.

## Quick-start commands

```bash
sbt core/test       # ~10s — fastest feedback
sbt tests/test      # ~25s — Postgres round-trip
sbt "core/test; tests/test; iron/test; circe/test; refined/test"  # full suite
```

## Don't forget

- The `BodyPart` type lives in `SelectBuilder` companion ([Select.scala:472](modules/core/src/main/scala/skunk/sharp/dsl/Select.scala#L472)).
- The encoder walker that does render-order dispatch is at [Select.scala:528](modules/core/src/main/scala/skunk/sharp/dsl/Select.scala#L528) onwards. It already handles N parts; the 3-slot limit is purely in the type signature.
- Match-type reduction inside Scala 3 sometimes fails on path-dependent types — if you hit `Concat[A, B]` not reducing, check whether you've path-prefixed `Where.Concat` somewhere it should be unprefixed.
- Operator precedence: `:=` has lower precedence than `&` in Scala. `c.email := Param[String] & c.age := Param[Int]` parses wrong. Always parens the assignments: `(c.email := Param[String]) & (c.age := Param[Int])`.
