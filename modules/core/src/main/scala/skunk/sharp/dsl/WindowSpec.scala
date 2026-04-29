package skunk.sharp.dsl

import skunk.{AppliedFragment, Fragment}
import skunk.sharp.TypedExpr

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
  private[dsl] val orderBys: List[OrderBy[?]],
  private[dsl] val frameOpt: Option[(FrameMode, FrameBound, FrameBound)]
) {

  def partitionBy(exprs: TypedExpr[?, ?]*): WindowSpec =
    new WindowSpec(partitionBys ++ exprs, orderBys, frameOpt)

  def orderBy(obs: OrderBy[?]*): WindowSpec =
    new WindowSpec(partitionBys, orderBys ++ obs, frameOpt)

  def rowsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Rows, start, end)))

  def rangeBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Range, start, end)))

  def groupsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Groups, start, end)))

  /**
   * Render the interior of `OVER (…)` as an `AppliedFragment`. PARTITION-BY / ORDER-BY items may carry
   * typed Args (Param-bearing); bind at Void here. Typed-args threading through window specs is roadmap.
   */
  private[sharp] def renderFragment: AppliedFragment = {
    val sections = scala.collection.mutable.ListBuffer[AppliedFragment]()
    if partitionBys.nonEmpty then {
      val items = TypedExpr.joined(partitionBys.map(e => SelectBuilder.bindVoid(e.fragment)), ", ")
      sections += (TypedExpr.raw("PARTITION BY ") |+| items)
    }
    if orderBys.nonEmpty then {
      val items = TypedExpr.joined(orderBys.map(o => SelectBuilder.bindVoid(o.fragment)), ", ")
      sections += (TypedExpr.raw("ORDER BY ") |+| items)
    }
    frameOpt.foreach { case (mode, start, end) =>
      sections += TypedExpr.raw(s"${mode.keyword} BETWEEN ${renderBound(start)} AND ${renderBound(end)}")
    }
    sections.toList match {
      case Nil          => AppliedFragment.empty
      case head :: Nil  => head
      case head :: tail => tail.foldLeft(head)((a, b) => a |+| TypedExpr.raw(" ") |+| b)
    }
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
  def orderBy(obs: OrderBy[?]*): WindowSpec               = empty.orderBy(obs*)

}

/** Append `OVER (spec)` to any expression. */
extension [T, A](expr: TypedExpr[T, A]) {

  def over(spec: WindowSpec): TypedExpr[T, A] = {
    // The window spec's args are baked at Void inside renderFragment; lift to Fragment[Void] and combine
    // with the LHS's typed Args.
    val innerVoid = TypedExpr.liftAfToVoid(spec.renderFragment)
    val withParens = TypedExpr.wrap(" OVER (", innerVoid, ")")
    val combined  = TypedExpr.combine(expr.fragment, withParens).asInstanceOf[Fragment[A]]
    TypedExpr[T, A](combined, expr.codec)
  }

  def over(): TypedExpr[T, A] = over(WindowSpec.empty)

}
