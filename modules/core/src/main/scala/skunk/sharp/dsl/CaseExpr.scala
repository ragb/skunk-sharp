package skunk.sharp.dsl

import skunk.{Codec, Fragment, Void}
import skunk.sharp.TypedExpr

/**
 * `CASE WHEN … THEN … [ELSE …] END` — the universal conditional primitive. Usable in SELECT projections,
 * WHERE, ORDER BY, UPDATE SET right-hand sides, GROUP BY — anywhere a [[TypedExpr]] is accepted.
 *
 * Branch predicates and bodies may carry their own `Args` (e.g. `Param[T]`). The compiled CASE expression
 * collapses to `Args = Void` — every branch's encoder is treated as Void-input via the [[TypedExpr.joinedVoid]]
 * Void-aware product chain. (Per-branch typed Args threading through CASE is roadmap; today, mixing
 * `Param[T]` and `Param.bind` value-RHS inside a CASE works at runtime but the result's typed Args is `Void`.)
 *
 * Terminate with `.otherwise(elseBranch)` (returns `TypedExpr[T, Void]`) or `.end` (returns
 * `TypedExpr[Option[T], Void]`).
 */
final class CaseWhen[T] private[sharp] (
  private val branches: List[(TypedExpr[Boolean, ?], TypedExpr[T, ?])],
  private val branchCodec: Codec[T]
) {

  /** Append another `WHEN` branch. */
  def when[A1, A2](cond: TypedExpr[Boolean, A1], branch: TypedExpr[T, A2]): CaseWhen[T] =
    new CaseWhen[T](branches :+ (cond, branch), branchCodec)

  /** `ELSE <branch> END` — all paths covered, result is always a value of `T`. */
  def otherwise[A](branch: TypedExpr[T, A]): TypedExpr[T, Void] =
    renderCaseWhen[T](branches, Some(branch), branchCodec)

  /** `END` without an ELSE — Postgres returns NULL when no branch matches, so result is `Option[T]`. */
  def end: TypedExpr[Option[T], Void] =
    renderCaseWhen[Option[T]](
      branches.map { case (c, b) => (c, b.asInstanceOf[TypedExpr[Option[T], ?]]) },
      None,
      branchCodec.opt
    )

}

private def renderCaseWhen[R](
  branches: List[(TypedExpr[Boolean, ?], TypedExpr[?, ?])],
  elseOpt:  Option[TypedExpr[?, ?]],
  codec0:   Codec[R]
): TypedExpr[R, Void] = {
  // Build an interleaved fragment list: ["CASE", " WHEN ", cond, " THEN ", body, ..., " ELSE ", else, " END"]
  // with each contributing to the SQL parts. Then fold via TypedExpr.combineSep at Void-Void to get a single
  // Fragment[Void] whose encoder properly contramaps Void to the nested-tuple shape the product expects.
  val pieces = scala.collection.mutable.ListBuffer[Fragment[?]](TypedExpr.voidFragment("CASE"))
  branches.foreach { case (cond, br) =>
    pieces += TypedExpr.voidFragment(" WHEN ")
    pieces += cond.fragment
    pieces += TypedExpr.voidFragment(" THEN ")
    pieces += br.fragment
  }
  elseOpt.foreach { e =>
    pieces += TypedExpr.voidFragment(" ELSE ")
    pieces += e.fragment
  }
  pieces += TypedExpr.voidFragment(" END")

  // joinedVoid handles the Void-Void Concat2 contramap chain — final fragment takes Void and supplies the
  // nested-Void tuple to the product chain.
  val combined: Fragment[Void] = TypedExpr.joinedVoid("", pieces.toList)
  TypedExpr[R, Void](combined, codec0)
}
