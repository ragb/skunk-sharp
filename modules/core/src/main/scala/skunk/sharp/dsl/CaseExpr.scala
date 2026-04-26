package skunk.sharp.dsl

import skunk.{Codec, Fragment}
import skunk.sharp.TypedExpr
import skunk.util.Origin

/**
 * `CASE WHEN … THEN … [ELSE …] END` — the universal conditional primitive. Usable in SELECT projections,
 * WHERE, ORDER BY, UPDATE SET right-hand sides, GROUP BY — anywhere a [[TypedExpr]] is accepted.
 *
 * {{{
 *   caseWhen(u.age < lit(18), lit("minor"))
 *     .when(u.age < lit(65), lit("adult"))
 *     .otherwise(lit("senior"))        // TypedExpr[String, ?]
 * }}}
 *
 * Each branch's predicate and body can carry their own `Args` (via `Param[T]` or other `TypedExpr[?, A]`); the
 * combined CASE expression's Args is widened to `?` (existential) — typed-args threading through CASE branches
 * is roadmap.
 *
 * Terminate with `.otherwise(elseBranch)` (returns `TypedExpr[T, ?]`) or `.end` (returns
 * `TypedExpr[Option[T], ?]`).
 */
final class CaseWhen[T] private[sharp] (
  private val branches: List[(TypedExpr[Boolean, ?], TypedExpr[T, ?])],
  private val branchCodec: Codec[T]
) {

  /** Append another `WHEN` branch. */
  def when[A1, A2](cond: TypedExpr[Boolean, A1], branch: TypedExpr[T, A2]): CaseWhen[T] =
    new CaseWhen[T](branches :+ (cond, branch), branchCodec)

  /** `ELSE <branch> END` — all paths covered, result is always a value of `T`. */
  def otherwise[A](branch: TypedExpr[T, A]): TypedExpr[T, ?] =
    renderCaseWhen[T](branches, Some(branch), branchCodec)

  /** `END` without an ELSE — Postgres returns NULL when no branch matches, so result is `Option[T]`. */
  def end: TypedExpr[Option[T], ?] =
    renderCaseWhen[Option[T]](
      branches.map { case (c, b) =>
        (c, TypedExpr[Option[T], Any](b.fragment.asInstanceOf[Fragment[Any]], b.codec.asInstanceOf[Codec[T]].opt.asInstanceOf[Codec[Option[T]]]))
      },
      None,
      branchCodec.opt
    )

}

private def renderCaseWhen[R](
  branches: List[(TypedExpr[Boolean, ?], TypedExpr[?, ?])],
  elseOpt:  Option[TypedExpr[?, ?]],
  codec0:   Codec[R]
): TypedExpr[R, ?] = {
  // Concatenate parts: CASE [WHEN <cond> THEN <branch>]* [ELSE <else>] END
  val partsBuf = scala.collection.mutable.ListBuffer[Either[String, cats.data.State[Int, String]]]()
  partsBuf += Left("CASE")
  // Accumulate encoder via Void-aware product across all sub-fragments.
  var enc: skunk.Encoder[Any] = skunk.Void.codec.asInstanceOf[skunk.Encoder[Any]]
  def addFragment(f: Fragment[?]): Unit = {
    partsBuf ++= f.parts
    val nextE = f.encoder.asInstanceOf[skunk.Encoder[Any]]
    if (!(nextE eq skunk.Void.codec)) {
      if (enc eq skunk.Void.codec) enc = nextE
      else                          enc = enc.product(nextE).asInstanceOf[skunk.Encoder[Any]]
    }
  }
  branches.foreach { case (cond, br) =>
    partsBuf += Left(" WHEN ")
    addFragment(cond.fragment)
    partsBuf += Left(" THEN ")
    addFragment(br.fragment)
  }
  elseOpt.foreach { e =>
    partsBuf += Left(" ELSE ")
    addFragment(e.fragment)
  }
  partsBuf += Left(" END")
  val frag: Fragment[Any] = Fragment(partsBuf.toList, enc, Origin.unknown)
  TypedExpr[R, Any](frag, codec0)
}
