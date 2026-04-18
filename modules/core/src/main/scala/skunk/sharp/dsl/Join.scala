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
          groupBys = Nil,
          havingOpt = None,
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
          groupBys = Nil,
          havingOpt = None,
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
  groupBys: List[TypedExpr[?]] = Nil,
  havingOpt: Option[Where] = None,
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
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[Where] = havingOpt,
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
      groupBys,
      havingOpt,
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

  /** `GROUP BY …`. Same semantics as [[SelectBuilder.groupBy]] — coverage is not type-checked. */
  def groupBy(
    f: JoinedView2[AL, CL, AR, CR] => TypedExpr[?] | Tuple
  ): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = {
    val fresh = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    copy(groupBys = groupBys ++ fresh)
  }

  /** `HAVING <predicate>`. Chains with AND. */
  def having(f: JoinedView2[AL, CL, AR, CR] => Where): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = {
    val next = havingOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(havingOpt = Some(next))
  }

  def limit(n: Int): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row]  = copy(limitOpt = Some(n))
  def offset(n: Int): ProjectedJoin2[RL, CL, AL, RR, CR0, AR, CR, Row] = copy(offsetOpt = Some(n))

  def compile: CompiledQuery[Row] = {
    val projList  = TypedExpr.joined(projections.map(_.render), ", ")
    val fromL     = TypedExpr.raw(aliasedFrom(left))
    val fromR     = TypedExpr.raw(aliasedFrom(right))
    val joinSql   = TypedExpr.raw(s" ${kind.sql} ") |+| fromR |+| TypedExpr.raw(" ON ") |+| onPred.render
    val header    = TypedExpr.raw("SELECT ") |+| projList |+| TypedExpr.raw(" FROM ") |+| fromL |+| joinSql
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withGroup =
      if (groupBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" GROUP BY ") |+| TypedExpr.joined(groupBys.map(_.render), ", ")
    val withHaving = havingOpt.fold(withGroup)(h => withGroup |+| TypedExpr.raw(" HAVING ") |+| h.render)
    val withOrder  =
      if (orderBys.isEmpty) withHaving
      else withHaving |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit  = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    CompiledQuery(withOffset, codec)
  }

}

/**
 * Render `"schema"."name" AS "alias"`, eliding the `AS` clause when the alias equals the relation's unqualified name
 * (the auto-alias case) — `"public"."posts"` already implies `"posts"` as the default alias.
 */
private[sharp] def aliasedFrom(ar: AliasedRelation[?, ?, ?]): String =
  if (ar.alias == ar.relation.name) ar.relation.qualifiedName
  else s"""${ar.relation.qualifiedName} AS "${ar.alias}""""

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

// ---- .innerJoin / .leftJoin ----

/**
 * Evidence that `T` is, or can be implicitly wrapped as, an [[AliasedRelation]]. Three sources:
 *   - An already-aliased relation (identity).
 *   - A [[Table]] `Table[Cols, Name]` — auto-aliased to the table's name.
 *   - A [[View]] `View[Cols, Name]` — auto-aliased to the view's name.
 *
 * Lets `users.innerJoin(posts)` work without explicit `.alias(...)`: the alias defaults to the relation's name, pulled
 * from the `Name` type parameter so it's usable as a NamedTuple label in the join's lambda view (`r.users.id`).
 */
sealed trait AsAliased[T] {
  type R <: Relation[Cols]
  type Cols <: Tuple
  type Alias <: String & Singleton
  def apply(t: T): AliasedRelation[R, Cols, Alias]
}

object AsAliased {

  type Aux[T, R_ <: Relation[Cols_], Cols_ <: Tuple, Alias_ <: String & Singleton] = AsAliased[T] {
    type R = R_
    type Cols = Cols_
    type Alias = Alias_
  }

  /** An already-aliased relation passes through unchanged. */
  given fromAliased[RR <: Relation[CC], CC <: Tuple, A <: String & Singleton]
    : AsAliased.Aux[AliasedRelation[RR, CC, A], RR, CC, A] = new AsAliased[AliasedRelation[RR, CC, A]] {
    type R = RR
    type Cols = CC
    type Alias = A
    def apply(a: AliasedRelation[RR, CC, A]): AliasedRelation[RR, CC, A] = a
  }

  /** A bare `Table` is auto-aliased to its own name. */
  given fromTable[CC <: Tuple, N <: String & Singleton]: AsAliased.Aux[Table[CC, N], Table[CC, N], CC, N] =
    new AsAliased[Table[CC, N]] {
      type R = Table[CC, N]
      type Cols = CC
      type Alias = N
      def apply(t: Table[CC, N]): AliasedRelation[Table[CC, N], CC, N] =
        new AliasedRelation[Table[CC, N], CC, N](t, t.name)
    }

  /** A bare `View` is auto-aliased to its own name. */
  given fromView[CC <: Tuple, N <: String & Singleton]: AsAliased.Aux[View[CC, N], View[CC, N], CC, N] =
    new AsAliased[View[CC, N]] {
      type R = View[CC, N]
      type Cols = CC
      type Alias = N
      def apply(v: View[CC, N]): AliasedRelation[View[CC, N], CC, N] =
        new AliasedRelation[View[CC, N], CC, N](v, v.name)
    }

}

/**
 * `.innerJoin` / `.leftJoin` on any relation-like value — bare `Table` / `View` (auto-aliased to its own name) or an
 * already-aliased `AliasedRelation`.
 */
extension [L](left: L)(using aL: AsAliased[L]) {

  /** `INNER JOIN` — right-side columns keep their declared types. */
  def innerJoin[R](right: R)(using aR: AsAliased[R])
    : IncompleteJoin2[aL.R, aL.Cols, aL.Alias, aR.R, aR.Cols, aR.Alias, aR.Cols] = {
    val al = aL(left)
    val ar = aR(right)
    new IncompleteJoin2[aL.R, aL.Cols, aL.Alias, aR.R, aR.Cols, aR.Alias, aR.Cols](
      al,
      ar,
      ar.relation.columns,
      JoinKind.Inner
    )
  }

  /** `LEFT JOIN` — right-side column value types are wrapped in `Option`; `.opt` on the codecs at runtime. */
  def leftJoin[R](right: R)(using aR: AsAliased[R])
    : IncompleteJoin2[aL.R, aL.Cols, aL.Alias, aR.R, aR.Cols, aR.Alias, NullableCols[aR.Cols]] = {
    val al        = aL(left)
    val ar        = aR(right)
    val nullified = nullabilifyCols(ar.relation.columns).asInstanceOf[NullableCols[aR.Cols]]
    new IncompleteJoin2[aL.R, aL.Cols, aL.Alias, aR.R, aR.Cols, aR.Alias, NullableCols[aR.Cols]](
      al,
      ar,
      nullified,
      JoinKind.Left
    )
  }

}
