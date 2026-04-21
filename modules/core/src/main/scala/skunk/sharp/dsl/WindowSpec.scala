package skunk.sharp.dsl

import skunk.AppliedFragment
import skunk.sharp.TypedExpr

/**
 * One bound on a window frame — used in `.rowsBetween` / `.rangeBetween` / `.groupsBetween`.
 *
 * {{{
 *   WindowSpec.orderBy(u.createdAt.asc).rowsBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow)
 * }}}
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
 * Start from the [[WindowSpec]] companion factory methods (or [[WindowSpec.empty]]), then chain:
 *
 *   - `.partitionBy(expr, …)` — `PARTITION BY` key expressions.
 *   - `.orderBy(expr.asc, …)` — `ORDER BY` sort terms.
 *   - `.rowsBetween` / `.rangeBetween` / `.groupsBetween` — explicit frame bounds.
 *
 * Pass the result to `.over(spec)` on any [[TypedExpr]]:
 *
 * {{{
 *   Pg.rowNumber.over(WindowSpec.partitionBy(u.dept).orderBy(u.salary.desc))
 *   Pg.sum(u.amount).over(WindowSpec.orderBy(u.createdAt.asc).rowsBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow))
 *   Pg.lag(u.age).over(WindowSpec.orderBy(u.age.asc))
 * }}}
 */
final class WindowSpec private[sharp] (
  private val partitionBys: List[TypedExpr[?]],
  private val orderBys: List[OrderBy],
  private val frameOpt: Option[(FrameMode, FrameBound, FrameBound)]
) {

  def partitionBy(exprs: TypedExpr[?]*): WindowSpec =
    new WindowSpec(partitionBys ++ exprs, orderBys, frameOpt)

  def orderBy(obs: OrderBy*): WindowSpec =
    new WindowSpec(partitionBys, orderBys ++ obs, frameOpt)

  def rowsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Rows, start, end)))

  def rangeBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Range, start, end)))

  def groupsBetween(start: FrameBound, end: FrameBound): WindowSpec =
    new WindowSpec(partitionBys, orderBys, Some((FrameMode.Groups, start, end)))

  /** Render the *interior* of the `OVER (…)` parens — called by the `.over` extension. */
  private[sharp] def render: AppliedFragment = {
    val parts = List.newBuilder[AppliedFragment]
    if partitionBys.nonEmpty then
      parts += TypedExpr.raw("PARTITION BY ") |+| TypedExpr.joined(partitionBys.map(_.render), ", ")
    if orderBys.nonEmpty then
      parts += TypedExpr.raw("ORDER BY ") |+| TypedExpr.joined(orderBys.map(_.af), ", ")
    frameOpt.foreach { case (mode, start, end) =>
      parts += TypedExpr.raw(s"${mode.keyword} BETWEEN ${renderBound(start)} AND ${renderBound(end)}")
    }
    TypedExpr.joined(parts.result(), " ")
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

  /** Empty spec — `OVER ()`. Useful for window functions that don't need partition or order. */
  val empty: WindowSpec = new WindowSpec(Nil, Nil, None)

  /** Start a spec with `PARTITION BY`. */
  def partitionBy(exprs: TypedExpr[?]*): WindowSpec = empty.partitionBy(exprs*)

  /** Start a spec with `ORDER BY`. */
  def orderBy(obs: OrderBy*): WindowSpec = empty.orderBy(obs*)

}

/**
 * Append `OVER (spec)` to any expression — the universal entry point for window functions.
 *
 * Works on both window-only functions (`Pg.rowNumber`, `Pg.rank`, …) and aggregate functions used in window position
 * (`Pg.sum(col)`, `Pg.count(col)`, …):
 *
 * {{{
 *   Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc))
 *   Pg.sum(u.salary).over(WindowSpec.partitionBy(u.dept).orderBy(u.salary.asc))
 *   Pg.rowNumber.over()                                           // OVER () — no partition, no order
 * }}}
 */
extension [T](expr: TypedExpr[T]) {

  def over(spec: WindowSpec): TypedExpr[T] =
    TypedExpr(expr.render |+| TypedExpr.raw(" OVER (") |+| spec.render |+| TypedExpr.raw(")"), expr.codec)

  /** `OVER ()` — empty window spec. */
  def over(): TypedExpr[T] = over(WindowSpec.empty)

}
