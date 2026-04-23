package skunk.sharp.dsl

import skunk.AppliedFragment
import skunk.sharp.*

/**
 * Marker for any relation whose FROM rendering references a CTE name rather than a base table. Both `CteRelation`
 * itself and re-aliased wrappers produced by `cteRelation.alias("x")` implement this trait so that
 * [[collectCtesInOrder]] can detect them regardless of how many `.alias` layers are on top.
 */
private[dsl] trait IsCte {
  def underlyingCte: CteRelation[?, ?]
}

/**
 * A named CTE — the value returned by [[cte]]. Extends [[Relation]] so it slots directly into `.select`, `.innerJoin`,
 * `.leftJoin`, and every other FROM-position verb.
 *
 * In a FROM clause it renders as just its name (`"name"` or `"name" AS "alias"`). The actual `WITH "name" AS (body)`
 * preamble is prepended once, at the outermost `.compile` call, by [[renderWithPreamble]].
 *
 * `deps` records which other CTEs this one directly references (captured at creation time). [[renderWithPreamble]] does
 * a depth-first walk over deps so chained CTEs (`cte2` whose body references `cte1`) are always emitted in the right
 * dependency order.
 */
final class CteRelation[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  val cteName: Name,
  private[sharp] val body: () => AppliedFragment,
  private[sharp] val deps: List[CteRelation[?, ?]],
  private[sharp] val cols0: Cols
) extends Relation[Cols] with IsCte {
  type Alias = Name
  type Mode  = AliasMode.Explicit

  def currentAlias: Name        = cteName
  def name: String              = cteName
  def schema: Option[String]    = None
  def columns: Cols             = cols0
  def expectedTableType: String = ""
  def underlyingCte: this.type  = this

  override def fromFragmentWith(a: String): AppliedFragment =
    if (a == cteName) TypedExpr.raw(s""""$cteName"""")
    else TypedExpr.raw(s""""$cteName" AS "$a"""")

}

/**
 * Lift a whole-row SELECT into a named CTE.
 *
 * {{{
 *   val active = cte("active", users.select.where(u => u.deleted_at.isNull))
 *   active.select.compile   // WITH "active" AS (SELECT … FROM "users" WHERE …) SELECT … FROM "active"
 * }}}
 *
 * The returned [[CteRelation]] can be joined, re-aliased, and used anywhere a [[Relation]] is accepted. Multiple CTEs
 * in the same query are collected and deduplicated at compile time — each `WITH` entry appears only once, in dependency
 * order.
 */
def cte[Ss <: Tuple, N <: String & Singleton](
  name: N,
  query: SelectBuilder[Ss]
)(using ev: IsSingleSource[Ss]): CteRelation[ev.Cols, N] = {
  val entries = query.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
  val deps    = directCtes(entries)
  val cols    = entries.head.effectiveCols.asInstanceOf[ev.Cols]
  new CteRelation(name, () => query.compileFragment(using ev), deps, cols)
}

/**
 * Lift a projected SELECT into a named CTE.
 *
 * Every column in the projection must carry a name — either a bare [[TypedColumn]] or an [[AliasedExpr]] — so the CTE
 * columns are well-defined. This is the same constraint as `ProjectedSelect.alias`.
 *
 * {{{
 *   val totals = cte("totals",
 *     orders.select(o => (o.user_id, Pg.sum(o.amount).as("total"))).groupBy(o => o.user_id)
 *   )
 *   totals.innerJoin(users).on(r => r.totals.user_id ==== r.users.id).select(r => (r.users.email, r.totals.total)).compile
 * }}}
 */
def cte[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, Row, N <: String & Singleton](
  name: N,
  query: ProjectedSelect[Ss, Proj, Groups, Row]
)(using
  gc: GroupCoverage[Proj, Groups],
  @scala.annotation.unused np: AllNamedProj[Proj]
): CteRelation[ProjCols[Proj], N] = {
  val entries = query.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
  val deps    = directCtes(entries)
  val cols    = buildProjectedCols(query.projections).asInstanceOf[ProjCols[Proj]]
  new CteRelation(name, () => query.compileFragment(using gc), deps, cols)
}

// ---- CTE collection helpers (used by SelectBuilder.compile and ProjectedSelect.compile) --------

/** Extract the directly-referenced CTEs from a source-entry list (handles re-aliased CteRelations via [[IsCte]]). */
private[dsl] def directCtes(entries: List[SourceEntry[?, ?, ?, ?]]): List[CteRelation[?, ?]] =
  entries.collect {
    case e if e.relation.isInstanceOf[IsCte] =>
      e.relation.asInstanceOf[IsCte].underlyingCte
  }

/**
 * Walk `entries`, find all CTE relations (transitively through their `deps`), and return them in dependency order
 * (earliest dependency first). Duplicate names are visited only once.
 */
private[dsl] def collectCtesInOrder(entries: List[SourceEntry[?, ?, ?, ?]]): List[CteRelation[?, ?]] = {
  val result                            = scala.collection.mutable.ListBuffer.empty[CteRelation[?, ?]]
  val visited                           = scala.collection.mutable.LinkedHashSet.empty[String]
  def visit(c: CteRelation[?, ?]): Unit =
    if (!visited.contains(c.cteName)) {
      visited += c.cteName
      c.deps.foreach(visit)
      result += c
    }
  directCtes(entries).foreach(visit)
  result.toList
}

/**
 * Render `WITH "name1" AS (body1), "name2" AS (body2) ` (with trailing space). Returns `None` when `ctes` is empty so
 * callers can skip prepending entirely rather than emitting an empty `WITH `.
 */
private[dsl] def renderWithPreamble(ctes: List[CteRelation[?, ?]]): Option[AppliedFragment] =
  if (ctes.isEmpty) None
  else {
    val defs = TypedExpr.joined(
      ctes.map(c => TypedExpr.raw(s""""${c.cteName}" AS (""") |+| c.body() |+| TypedExpr.raw(")")),
      ", "
    )
    Some(TypedExpr.raw("WITH ") |+| defs |+| TypedExpr.raw(" "))
  }
