package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.TypedExpr

/**
 * `CASE WHEN … THEN … [ELSE …] END` — the universal conditional primitive. Usable in SELECT projections, WHERE, ORDER
 * BY, UPDATE SET right-hand sides, GROUP BY — anywhere a [[TypedExpr]] is accepted.
 *
 * {{{
 *   caseWhen(u.age < 18, lit("minor"))
 *     .when(u.age < 65, lit("adult"))
 *     .otherwise(lit("senior"))        // TypedExpr[String]
 * }}}
 *
 * Terminate with either `.otherwise(elseBranch)` (returns `TypedExpr[T]`) or `.end` (returns `TypedExpr[Option[T]]` —
 * Postgres returns NULL when no branch matches and there's no ELSE).
 *
 * We ship only the **searched** form (arbitrary boolean per branch). SQL also has a **simple** switch-style form (`CASE
 * target WHEN v1 THEN … END`) but it's strict sugar — `CASE target WHEN v THEN b END` is exactly
 * `CASE WHEN target = v THEN b END`, and Postgres plans them identically. One verb keeps the DSL surface small.
 *
 * The branch output type `T` is inferred from the first `(condition, branch)` pair; subsequent `.when(..., b)` calls
 * require `b: TypedExpr[T]`. A mismatched branch is a plain Scala type error — no match type needed.
 *
 * CASE is a SQL keyword-expression, not a function, so the entry point `caseWhen` lives at the top of the DSL surface
 * (`import skunk.sharp.dsl.*`) alongside `lit` / `param` / `Table`, rather than under `Pg.…`.
 */
final class CaseWhen[T] private[sharp] (
  private val branches: List[(TypedExpr[Boolean], TypedExpr[T])],
  private val branchCodec: Codec[T]
) {

  /** Append another `WHEN` branch. Accepts any `TypedExpr[Boolean]` (including any `Where[A]`). */
  def when(cond: TypedExpr[Boolean], branch: TypedExpr[T]): CaseWhen[T] =
    new CaseWhen[T](branches :+ (cond, branch), branchCodec)

  /** `ELSE <branch> END` — all paths covered, result is always a value of `T`. */
  def otherwise(branch: TypedExpr[T]): TypedExpr[T] =
    renderCaseWhen[T](branches, Some(branch), branchCodec)

  /** `END` without an ELSE — Postgres returns NULL when no branch matches, so result is `Option[T]`. */
  def end: TypedExpr[Option[T]] =
    renderCaseWhen[Option[T]](
      branches.map { case (c, b) =>
        (c, TypedExpr(b.render, b.codec.asInstanceOf[Codec[T]]).asInstanceOf[TypedExpr[Option[T]]])
      },
      None,
      branchCodec.opt
    )

}

private def renderCaseWhen[R](
  branches: List[(TypedExpr[Boolean], TypedExpr[?])],
  elseOpt: Option[TypedExpr[?]],
  codec0: Codec[R]
): TypedExpr[R] =
  TypedExpr(
    {
      val head      = TypedExpr.raw("CASE")
      val whenParts = branches.foldLeft(head) { case (acc, (cond, br)) =>
        acc |+|
          TypedExpr.raw(" WHEN ") |+| cond.render |+|
          TypedExpr.raw(" THEN ") |+| br.render
      }
      val withElse = elseOpt.fold(whenParts)(e => whenParts |+| TypedExpr.raw(" ELSE ") |+| e.render)
      withElse |+| TypedExpr.raw(" END")
    },
    codec0
  )
