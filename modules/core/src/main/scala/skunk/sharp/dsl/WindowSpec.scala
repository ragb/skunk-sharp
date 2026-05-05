package skunk.sharp.dsl

import skunk.{Fragment, Void}
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where
import skunk.util.Origin

/**
 * One bound on a window frame — used in `.rowsBetween` / `.rangeBetween` / `.groupsBetween`.
 */
enum FrameBound:
  case UnboundedPreceding
  case Preceding(n: Int)
  case CurrentRow
  case Following(n: Int)
  case UnboundedFollowing

private enum FrameMode(val keyword: String):
  case Rows   extends FrameMode("ROWS")
  case Range  extends FrameMode("RANGE")
  case Groups extends FrameMode("GROUPS")

/**
 * Builder for the content of an `OVER (…)` clause.
 *
 * Args of partition-by and order-by items thread into the wrapping `over` extension's result Args via the
 * combined `Concat[PA, OA]` slot. Param-bearing items (`Param[Int].asc`, `partitionBy(Param[String])`) surface
 * as typed `Args` on the outer query. Frame bounds are static integer constants — Args-neutral.
 */
final class WindowSpec[PA, OA] private[sharp] (
  // Comma-joined PARTITION BY items (without the leading `PARTITION BY ` keyword), or None when empty.
  private[dsl] val pbItems: Option[Fragment[PA]],
  // Comma-joined ORDER BY items (without the leading `ORDER BY ` keyword), or None when empty.
  private[dsl] val obItems: Option[Fragment[OA]],
  private[dsl] val frameOpt: Option[(FrameMode, FrameBound, FrameBound)]
) {

  def partitionBy[B](e: TypedExpr[?, B])(using
    c2: Where.Concat2[PA, B]
  ): WindowSpec[Where.Concat[PA, B], OA] = {
    val combined: Fragment[Where.Concat[PA, B]] = pbItems match {
      case None       => e.fragment.asInstanceOf[Fragment[Where.Concat[PA, B]]]
      case Some(prev) => TypedExpr.combineSep[PA, B](prev, ", ", e.fragment)
    }
    new WindowSpec(Some(combined), obItems, frameOpt)
  }

  def orderBy[B](o: OrderBy[B])(using
    c2: Where.Concat2[OA, B]
  ): WindowSpec[PA, Where.Concat[OA, B]] = {
    val combined: Fragment[Where.Concat[OA, B]] = obItems match {
      case None       => o.fragment.asInstanceOf[Fragment[Where.Concat[OA, B]]]
      case Some(prev) => TypedExpr.combineSep[OA, B](prev, ", ", o.fragment)
    }
    new WindowSpec(pbItems, Some(combined), frameOpt)
  }

  def rowsBetween(start: FrameBound, end: FrameBound): WindowSpec[PA, OA] =
    new WindowSpec(pbItems, obItems, Some((FrameMode.Rows, start, end)))

  def rangeBetween(start: FrameBound, end: FrameBound): WindowSpec[PA, OA] =
    new WindowSpec(pbItems, obItems, Some((FrameMode.Range, start, end)))

  def groupsBetween(start: FrameBound, end: FrameBound): WindowSpec[PA, OA] =
    new WindowSpec(pbItems, obItems, Some((FrameMode.Groups, start, end)))

  /**
   * Render the interior of `OVER (…)` as a typed `Fragment[Concat[PA, OA]]`. PARTITION-BY items appear first
   * (typed via `PA`), then ORDER-BY items (typed via `OA`), then the frame clause (Args-neutral). Param-bearing
   * items have their typed Args threaded into the outer query's `Args` via `Concat`.
   */
  private[sharp] def renderTyped(using
    c2: Where.Concat2[PA, OA]
  ): Fragment[Where.Concat[PA, OA]] = {
    val frameSql: String = frameOpt.fold("") { case (mode, start, end) =>
      s" ${mode.keyword} BETWEEN ${WindowSpec.renderBound(start)} AND ${WindowSpec.renderBound(end)}"
    }
    (pbItems, obItems) match {
      case (None, None) =>
        TypedExpr.voidFragment(frameSql).asInstanceOf[Fragment[Where.Concat[PA, OA]]]
      case (Some(pb), None) =>
        // OA = Void here, so Concat[PA, Void] = PA — cast safe.
        TypedExpr.wrap("PARTITION BY ", pb, frameSql).asInstanceOf[Fragment[Where.Concat[PA, OA]]]
      case (None, Some(ob)) =>
        // PA = Void here, so Concat[Void, OA] = OA — cast safe.
        TypedExpr.wrap("ORDER BY ", ob, frameSql).asInstanceOf[Fragment[Where.Concat[PA, OA]]]
      case (Some(pb), Some(ob)) =>
        val pbPrefixed = TypedExpr.wrap("PARTITION BY ", pb, "")
        val combined   = TypedExpr.combineSep[PA, OA](pbPrefixed, " ORDER BY ", ob)
        if (frameSql.isEmpty) combined else TypedExpr.wrap("", combined, frameSql)
    }
  }

}

object WindowSpec {

  val empty: WindowSpec[Void, Void] = new WindowSpec(None, None, None)

  def partitionBy[B](e: TypedExpr[?, B])(using
    c2: Where.Concat2[Void, B]
  ): WindowSpec[Where.Concat[Void, B], Void] = empty.partitionBy(e)

  def orderBy[B](o: OrderBy[B])(using
    c2: Where.Concat2[Void, B]
  ): WindowSpec[Void, Where.Concat[Void, B]] = empty.orderBy(o)

  private[dsl] def renderBound(b: FrameBound): String = b match {
    case FrameBound.UnboundedPreceding => "UNBOUNDED PRECEDING"
    case FrameBound.Preceding(n)       => s"$n PRECEDING"
    case FrameBound.CurrentRow         => "CURRENT ROW"
    case FrameBound.Following(n)       => s"$n FOLLOWING"
    case FrameBound.UnboundedFollowing => "UNBOUNDED FOLLOWING"
  }

}

/** Append `OVER (spec)` to any expression. */
extension [T, A](expr: TypedExpr[T, A]) {

  def over[PA, OA](spec: WindowSpec[PA, OA])(using
    paOa: Where.Concat2[PA, OA],
    full: Where.Concat2[A, Where.Concat[PA, OA]]
  ): TypedExpr[T, Where.Concat[A, Where.Concat[PA, OA]]] = {
    val inner   = spec.renderTyped
    val wrapped = TypedExpr.wrap(" OVER (", inner, ")")
    val combined = TypedExpr.combine[A, Where.Concat[PA, OA]](expr.fragment, wrapped)
    TypedExpr(combined, expr.codec)
  }

  /** `OVER ()` — empty window spec; Args of the result is the LHS expression's Args unchanged. */
  def over(): TypedExpr[T, A] = {
    val parts = expr.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(" OVER ()"))
    val frag  = Fragment(parts, expr.fragment.encoder, Origin.unknown)
    TypedExpr(frag, expr.codec)
  }

}
