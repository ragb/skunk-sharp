package skunk.sharp.dsl

import skunk.Fragment
import skunk.sharp.TypedExpr
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
 * Args of partition / order expressions thread into the wrapping `over` extension's result Args via
 * fragment encoder products. Currently surfaced as existential `?` on `over`'s output (per-arg threading
 * roadmap; in the meantime, partition by `Param` works at runtime — encoder accumulates correctly — but
 * the static Args type is widened).
 */
final class WindowSpec private[sharp] (
  private[dsl] val partitionBys: List[TypedExpr[?, ?]],
  private[dsl] val orderBys: List[OrderBy],
  private[dsl] val frameOpt: Option[(FrameMode, FrameBound, FrameBound)]
) {

  def partitionBy(exprs: TypedExpr[?, ?]*): WindowSpec =
    new WindowSpec(partitionBys ++ exprs, orderBys, frameOpt)

  def orderBy(obs: OrderBy*): WindowSpec =
    new WindowSpec(partitionBys, orderBys ++ obs, frameOpt)

  def rowsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Rows, start, end)))

  def rangeBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Range, start, end)))

  def groupsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Groups, start, end)))

  /** Render the *interior* of the `OVER (…)` parens as a typed Fragment. */
  private[sharp] def renderFragment: Fragment[?] = {
    val sections = scala.collection.mutable.ListBuffer[Fragment[?]]()
    if partitionBys.nonEmpty then {
      val joined = SelectBuilder.joinFragments(partitionBys.map(_.fragment), ", ")
      sections += joined.foldLeft[Fragment[?]](TypedExpr.voidFragment("PARTITION BY "))((acc, p) =>
        joinTwo(acc, p)
      )
    }
    if orderBys.nonEmpty then {
      val joined = SelectBuilder.joinFragments(orderBys.map(_.fragment), ", ")
      sections += joined.foldLeft[Fragment[?]](TypedExpr.voidFragment("ORDER BY "))((acc, p) =>
        joinTwo(acc, p)
      )
    }
    frameOpt.foreach { case (mode, start, end) =>
      sections += TypedExpr.voidFragment(s"${mode.keyword} BETWEEN ${renderBound(start)} AND ${renderBound(end)}")
    }
    sections.toList match {
      case Nil          => TypedExpr.voidFragment("")
      case head :: Nil  => head
      case head :: tail => tail.foldLeft(head)((a, b) => joinTwo(joinTwo(a, TypedExpr.voidFragment(" ")), b))
    }
  }

  private def joinTwo(a: Fragment[?], b: Fragment[?]): Fragment[?] = {
    val parts = a.parts ++ b.parts
    val enc   = SelectBuilder.combineEncoders(a.encoder, b.encoder)
    Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
  }

  private def renderBound(b: FrameBound): String = b match {
    case FrameBound.UnboundedPreceding => "UNBOUNDED PRECEDING"
    case FrameBound.Preceding(n)       => s"$n PRECEDING"
    case FrameBound.CurrentRow         => "CURRENT ROW"
    case FrameBound.Following(n)       => s"$n FOLLOWING"
    case FrameBound.UnboundedFollowing => "UNBOUNDED FOLLOWING"
  }

}

object WindowSpec {

  val empty: WindowSpec = new WindowSpec(Nil, Nil, None)

  def partitionBy(exprs: TypedExpr[?, ?]*): WindowSpec = empty.partitionBy(exprs*)
  def orderBy(obs: OrderBy*): WindowSpec               = empty.orderBy(obs*)

}

/** Append `OVER (spec)` to any expression. */
extension [T, A](expr: TypedExpr[T, A]) {

  def over(spec: WindowSpec): TypedExpr[T, ?] = {
    val inner = spec.renderFragment
    val parts = expr.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(" OVER (")) ++
      inner.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val enc   = SelectBuilder.combineEncoders(expr.fragment.encoder, inner.encoder)
    val frag  = Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[Any]]
    TypedExpr[T, Any](frag, expr.codec)
  }

  def over(): TypedExpr[T, ?] = over(WindowSpec.empty)

}
