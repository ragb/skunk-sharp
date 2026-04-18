package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.*
import skunk.sharp.internal.tupleCodec
import skunk.sharp.where.Where

import scala.NamedTuple

/**
 * Two-table SQL joins (v1). Kept as a sibling of [[SelectBuilder]] while the unified "one builder for 1..N sources"
 * story is being designed. All join-specific methods are shaped identically to the single-source builder (`.where`,
 * `.orderBy`, `.limit`, `.offset`, `.select`) so users see one API surface.
 *
 * {{{
 *   users.alias("u").innerJoin(posts.alias("p")).on(r => r.u.id ==== r.p.user_id)
 *     .select(r => (mail = r.u.email, title = r.p.title))
 *     .where(r => r.p.published === true)
 *     .orderBy(r => r.p.created_at.desc)
 *     .compile
 * }}}
 *
 *   - `.alias("u")` wraps any [[Relation]] in an [[AliasedRelation]] carrying the alias at the type level.
 *   - `.innerJoin` / `.leftJoin` on an aliased relation take another aliased relation and return [[IncompleteJoin2]]
 *     (must call `.on(…)` before you can execute or project — compile error otherwise).
 *   - `.on(r => pred)` finalises the join predicate, returning [[Join2]] which exposes `.where`, `.orderBy`, `.limit`,
 *     `.offset`, and `.select(f)` / `.apply(f)` for projection.
 *   - The `r` view is a named tuple keyed by alias: `r.<alias>.<column>`.
 *   - `LEFT JOIN` flips the right-hand-side columns' nullability for `.where` / `.select` — value types become
 *     `Option[T]` and codecs get `.opt` at runtime. The `.on` predicate still sees the declared (non-nullable) types
 *     because Postgres evaluates `ON` before padding unmatched rows with `NULL`s.
 *
 * **Roadmap:** unify into `SelectBuilder[Sources <: Tuple]` so single-table and join cases share one class. Requires a
 * match type that reduces the view to `ColumnsView[Cols]` for 1 source and a named-tuple-of-views for N sources.
 */

/** A relation paired with its SQL alias (`"u"`, `"p"`, `"sub"`, …). Constructed via the `.alias("u")` extension. */
final class AliasedRelation[R <: Relation[Cols], Cols <: Tuple, Alias <: String & Singleton] private[sharp] (
  val relation: R,
  val alias: Alias
)

/**
 * `.alias("u")` on any relation lifts it into an [[AliasedRelation]]. Named `.alias` (not `.as`) to keep a clean
 * separation from [[TypedExpr.as]], which renames a projected column in the SQL — a different operation.
 */
extension [R <: Relation[Cols], Cols <: Tuple](r: R) {

  def alias[Alias <: String & Singleton](a: Alias): AliasedRelation[R, Cols, Alias] =
    new AliasedRelation[R, Cols, Alias](r, a)

}

/** Kind of join — drives the rendered keyword and whether the right-side cols are nullabilified. */
enum JoinKind(val sql: String) {
  case Inner extends JoinKind("INNER JOIN")
  case Left  extends JoinKind("LEFT JOIN")
}

/**
 * Type-level "make every column nullable" — wraps each value type in `Option` and flips the Null phantom to `true`.
 * Used for the RIGHT side of a LEFT JOIN, where all columns may be `NULL` when there's no matching row.
 */
type NullableCols[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, d] *: tail => Column[Option[t], n, true, d] *: NullableCols[tail]
  case EmptyTuple                  => EmptyTuple
}

/**
 * Runtime counterpart to [[NullableCols]]: walk the columns tuple and wrap each codec in `.opt` so the decoder emits
 * `Option[T]`. Name / tpe / flags are preserved.
 */
private[sharp] def nullabilifyCols(cols: Tuple): Tuple = {
  val wrapped = cols.toList.map {
    case c: Column[?, ?, ?, ?] =>
      Column[Any, "x", Boolean, Boolean](
        name = c.name.asInstanceOf["x"],
        tpe = c.tpe,
        codec = c.codec.asInstanceOf[Codec[Any]].opt.asInstanceOf[Codec[Any]],
        isNullable = true,
        hasDefault = c.hasDefault,
        isPrimary = c.isPrimary,
        isUnique = c.isUnique
      )
    case other =>
      other
  }
  Tuple.fromArray(wrapped.toArray[Any])
}

/**
 * Named-tuple view for a two-table join, keyed by alias. Lambdas receive `r` where `r.<leftAlias>` and `r.<rightAlias>`
 * are the per-relation [[ColumnsView]]s.
 */
type JoinedView2[AL <: String & Singleton, CL <: Tuple, AR <: String & Singleton, CR <: Tuple] =
  NamedTuple.NamedTuple[(AL, AR), (ColumnsView[CL], ColumnsView[CR])]

/**
 * Incomplete two-table join — the shape after `.innerJoin(other)` / `.leftJoin(other)`, before `.on(predicate)`.
 * Deliberately exposes only `.on` so compiling without a predicate is a compile error.
 */
final class IncompleteJoin2[
  RL <: Relation[CL],
  CL <: Tuple,
  AL <: String & Singleton,
  RR <: Relation[CR0],
  CR0 <: Tuple,
  AR <: String & Singleton,
  CR <: Tuple // effective right-side Cols after nullability flipping
] private[sharp] (
  left: AliasedRelation[RL, CL, AL],
  right: AliasedRelation[RR, CR0, AR],
  rightColsEffective: CR,
  kind: JoinKind
) {

  /**
   * Finalise the join predicate. The view here uses the right relation's **original** columns (non-nullabilified) —
   * Postgres evaluates `ON` before padding unmatched rows with `NULL`s, so on both sides the declared types apply.
   * Subsequent `.where` / `.select` clauses see the LEFT-JOIN-nullabilified shape.
   */
  def on(f: JoinedView2[AL, CL, AR, CR0] => Where): Join2[RL, CL, AL, RR, CR0, AR, CR] = {
    val onView = joinedView2[AL, CL, AR, CR0](
      left.alias,
      left.relation.columns.asInstanceOf[CL],
      right.alias,
      right.relation.columns
    )
    val pred = f(onView)
    new Join2[RL, CL, AL, RR, CR0, AR, CR](left, right, rightColsEffective, kind, pred)
  }

}

/**
 * Fully-specified two-table join. Supports `.where` / `.orderBy` / `.limit` / `.offset`, and projection via `.apply`.
 */
final class Join2[
  RL <: Relation[CL],
  CL <: Tuple,
  AL <: String & Singleton,
  RR <: Relation[CR0],
  CR0 <: Tuple,
  AR <: String & Singleton,
  CR <: Tuple
] private[sharp] (
  private[sharp] val left: AliasedRelation[RL, CL, AL],
  private[sharp] val right: AliasedRelation[RR, CR0, AR],
  private[sharp] val rightColsEffective: CR,
  private[sharp] val kind: JoinKind,
  private[sharp] val onPred: Where,
  private[sharp] val whereOpt: Option[Where] = None,
  private[sharp] val orderBys: List[OrderBy] = Nil,
  private[sharp] val limitOpt: Option[Int] = None,
  private[sharp] val offsetOpt: Option[Int] = None
) {

  private def copy(
    whereOpt: Option[Where] = whereOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt
  ): Join2[RL, CL, AL, RR, CR0, AR, CR] =
    new Join2[RL, CL, AL, RR, CR0, AR, CR](
      left,
      right,
      rightColsEffective,
      kind,
      onPred,
      whereOpt,
      orderBys,
      limitOpt,
      offsetOpt
    )

  private def view: JoinedView2[AL, CL, AR, CR] =
    joinedView2[AL, CL, AR, CR](
      left.alias,
      left.relation.columns.asInstanceOf[CL],
      right.alias,
      rightColsEffective
    )

  def where(f: JoinedView2[AL, CL, AR, CR] => Where): Join2[RL, CL, AL, RR, CR0, AR, CR] = {
    val next = whereOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(whereOpt = Some(next))
  }

  def orderBy(f: JoinedView2[AL, CL, AR, CR] => OrderBy | Tuple): Join2[RL, CL, AL, RR, CR0, AR, CR] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  def limit(n: Int): Join2[RL, CL, AL, RR, CR0, AR, CR]  = copy(limitOpt = Some(n))
  def offset(n: Int): Join2[RL, CL, AL, RR, CR0, AR, CR] = copy(offsetOpt = Some(n))

  /**
   * Projection — pick the columns / expressions to return. Same shape as [[SelectBuilder.apply]] on a single relation:
   * single `TypedExpr[T]` → row is `T`; tuple → row is the tuple of output types.
   */
  transparent inline def select[X](inline f: JoinedView2[AL, CL, AR, CR] => X)
    : ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, ProjResult[X]] = {
    val v = view
    f(v) match {
      case expr: TypedExpr[?] =>
        new ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, ProjResult[X]](
          left,
          right,
          rightColsEffective,
          kind,
          onPred,
          whereOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]]
        )
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, ProjResult[X]](
          left,
          right,
          rightColsEffective,
          kind,
          onPred,
          whereOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          exprs,
          codec
        )
    }
  }

  transparent inline def apply[X](inline f: JoinedView2[AL, CL, AR, CR] => X)
    : ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, ProjResult[X]] = select[X](f)

}

/** Projected join — a [[Join2]] plus a chosen projection. Terminal: compile to a [[CompiledQuery]]. */
final class ProjectedJoin2[
  RL <: Relation[CL],
  CL <: Tuple,
  AL <: String & Singleton,
  RR <: Relation[CR0],
  CR0 <: Tuple,
  AR <: String & Singleton,
  CR <: Tuple,
  Row
](
  left: AliasedRelation[RL, CL, AL],
  right: AliasedRelation[RR, CR0, AR],
  rightColsEffective: CR,
  kind: JoinKind,
  onPred: Where,
  whereOpt: Option[Where],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  projections: List[TypedExpr[?]],
  codec: Codec[Row]
) {

  private def view: JoinedView2[AL, CL, AR, CR] =
    joinedView2[AL, CL, AR, CR](
      left.alias,
      left.relation.columns.asInstanceOf[CL],
      right.alias,
      rightColsEffective
    )

  private def copy(
    whereOpt: Option[Where] = whereOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt
  ): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] =
    new ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row](
      left,
      right,
      rightColsEffective,
      kind,
      onPred,
      whereOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      projections,
      codec
    )

  def where(f: JoinedView2[AL, CL, AR, CR] => Where): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = {
    val next = whereOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(whereOpt = Some(next))
  }

  def orderBy(f: JoinedView2[AL, CL, AR, CR] => OrderBy | Tuple): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  def limit(n: Int): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row]  = copy(limitOpt = Some(n))
  def offset(n: Int): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = copy(offsetOpt = Some(n))

  def compile: CompiledQuery[Row] = {
    val projList  = TypedExpr.joined(projections.map(_.render), ", ")
    val fromL     = TypedExpr.raw(s"""${left.relation.qualifiedName} AS "${left.alias}"""")
    val fromR     = TypedExpr.raw(s"""${right.relation.qualifiedName} AS "${right.alias}"""")
    val joinSql   = TypedExpr.raw(s" ${kind.sql} ") |+| fromR |+| TypedExpr.raw(" ON ") |+| onPred.render
    val header    = TypedExpr.raw("SELECT ") |+| projList |+| TypedExpr.raw(" FROM ") |+| fromL |+| joinSql
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withOrder =
      if (orderBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit  = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    CompiledQuery(withOffset, codec)
  }

}

/** Build the named-tuple view of two aliased relations. Columns render with their alias prefix. */
private[sharp] def joinedView2[
  AL <: String & Singleton,
  CL <: Tuple,
  AR <: String & Singleton,
  CR <: Tuple
](aliasL: AL, colsL: CL, aliasR: AR, colsR: CR): JoinedView2[AL, CL, AR, CR] = {
  val vL = ColumnsView.qualified(colsL, aliasL)
  val vR = ColumnsView.qualified(colsR, aliasR)
  (vL, vR).asInstanceOf[JoinedView2[AL, CL, AR, CR]]
}

// ---- .innerJoin / .leftJoin on an AliasedRelation ----

extension [RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton](left: AliasedRelation[RL, CL, AL]) {

  /** `INNER JOIN` — right-side columns keep their declared types. */
  def innerJoin[RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](
    right: AliasedRelation[RR, CR, AR]
  ): IncompleteJoin2[RL, CL, AL, RR, CR, AR, CR] =
    new IncompleteJoin2[RL, CL, AL, RR, CR, AR, CR](left, right, right.relation.columns, JoinKind.Inner)

  /** `LEFT JOIN` — right-side column value types are wrapped in `Option`; `.opt` on the codecs at runtime. */
  def leftJoin[RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](
    right: AliasedRelation[RR, CR, AR]
  ): IncompleteJoin2[RL, CL, AL, RR, CR, AR, NullableCols[CR]] = {
    val nullified = nullabilifyCols(right.relation.columns).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin2[RL, CL, AL, RR, CR, AR, NullableCols[CR]](left, right, nullified, JoinKind.Left)
  }

}
