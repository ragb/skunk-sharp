package skunk.sharp.dsl

import skunk.AppliedFragment
import skunk.sharp.*

/**
 * A derived relation — the result of a `SELECT` used as a subquery in FROM / JOIN position. Renders as
 * `(<inner SQL>) AS "<alias>"` in the outer FROM clause, carrying its parameters through unchanged.
 *
 * Holds the inner source **lazily** as a thunk: the inner SQL is only materialised when the *outer* query's `.compile`
 * walks its sources. This means the user only writes one `.compile` per query tree — intermediate `.asRelation(name)`
 * steps never force early rendering.
 *
 * Constructed via the `.asRelation("name")` extension on a single-source whole-row [[SelectBuilder]]. Future:
 * projection-typed `.asRelation` that derives a smaller `Cols` from `.select(u => …)` expressions.
 */
final class SelectRelation[Cols <: Tuple, N <: String & Singleton] private[sharp] (
  val name: N,
  val columns: Cols,
  private[sharp] val renderInner: () => AppliedFragment
) extends Relation[Cols] {

  val schema: Option[String]    = None
  val expectedTableType: String = ""

  override def qualifiedName: String = s""""$name""""

  /**
   * Always emit `(<inner>) AS "<alias>"` — Postgres requires a derived table to have an alias, and the outer parens
   * isolate the inner SELECT's clauses from the outer query's. The inner SQL is materialised *now*, at outer-compile
   * time, via the stored thunk.
   */
  override def fromFragment(alias: String): AppliedFragment =
    TypedExpr.raw("(") |+| renderInner() |+| TypedExpr.raw(s""") AS "$alias"""")

}

/**
 * `.asRelation("alias")` on a single-source whole-row [[SelectBuilder]] — name the query as a joinable source. The
 * inner SelectBuilder is captured by closure; its compilation runs only when the outer query is compiled.
 *
 * {{{
 *   val active = users.select.where(u => u.deleted_at.isNull).asRelation("active")
 *   active.innerJoin(orders).on(r => r.active.id ==== r.orders.user_id).compile
 *   // single `.compile` at the end — the inner WHERE is rendered as part of the outer compilation.
 * }}}
 *
 * Available only on single-source whole-row builders today via `IsSingleSource` evidence. Projection-typed subqueries
 * aren't supported yet — use core's subquery-in-expression form (`sb.asExpr`) for scalar subqueries.
 */
extension [Ss <: Tuple](sb: SelectBuilder[Ss])(using ev: IsSingleSource[Ss]) {

  def asRelation[N <: String & Singleton](n: N): SelectRelation[ev.Cols, N] = {
    val cols = sb.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].head.effectiveCols.asInstanceOf[ev.Cols]
    new SelectRelation[ev.Cols, N](n, cols, () => sb.compile(using ev).af)
  }

}
