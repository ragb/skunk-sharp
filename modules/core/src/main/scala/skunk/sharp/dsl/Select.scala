package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec}
import skunk.sharp.where.{&&, Where}

/**
 * Unified SELECT builder — one class for single-source, JOINed, or CROSS-joined queries. Every lambda-taking method
 * receives a [[SelectView]], which the match type reduces to:
 *
 *   - `ColumnsView[Cols]` when there's exactly one source — user writes `u.email`, same as before.
 *   - `JoinedView[Ss]` (a Scala 3 named tuple keyed by alias) when there are 2+ sources — user writes `r.users.email`.
 *
 * Row-level locking (`FOR UPDATE`, `FOR SHARE`, …) is gated on `IsSingleTable[Ss]` evidence — only a single-source
 * query over a [[Table]] can lock; anything else is a compile error.
 */
final class SelectBuilder[Ss <: Tuple] private[sharp] (
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean = false,
  private[sharp] val whereOpt: Option[Where] = None,
  private[sharp] val groupBys: List[TypedExpr[?]] = Nil,
  private[sharp] val havingOpt: Option[Where] = None,
  private[sharp] val orderBys: List[OrderBy] = Nil,
  private[sharp] val limitOpt: Option[Int] = None,
  private[sharp] val offsetOpt: Option[Int] = None,
  private[sharp] val lockingOpt: Option[Locking] = None
) {

  private def copy(
    distinct: Boolean = distinct,
    whereOpt: Option[Where] = whereOpt,
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[Where] = havingOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt
  ): SelectBuilder[Ss] =
    new SelectBuilder[Ss](sources, distinct, whereOpt, groupBys, havingOpt, orderBys, limitOpt, offsetOpt, lockingOpt)

  private def view: SelectView[Ss] = buildSelectView[Ss](sources)

  def where(f: SelectView[Ss] => Where): SelectBuilder[Ss] = {
    val next = whereOpt.fold(f(view))(_ && f(view))
    copy(whereOpt = Some(next))
  }

  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): SelectBuilder[Ss] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY …` on a pre-projection builder — runtime-only. Coverage of the later projection isn't type-checked here
   * because we don't know the projection yet; use the `.select(...).groupBy(...)` order (or the whole-row [[compile]])
   * to get the compile-time check via [[GroupCoverage]].
   */
  def groupBy(f: SelectView[Ss] => TypedExpr[?] | Tuple): SelectBuilder[Ss] = {
    val fresh = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    copy(groupBys = groupBys ++ fresh)
  }

  /** `HAVING <predicate>`. Chains with AND. */
  def having(f: SelectView[Ss] => Where): SelectBuilder[Ss] = {
    val next = havingOpt.fold(f(view))(_ && f(view))
    copy(havingOpt = Some(next))
  }

  def limit(n: Int): SelectBuilder[Ss]  = copy(limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[Ss] = copy(offsetOpt = Some(n))

  /** `SELECT DISTINCT …`. */
  def distinctRows: SelectBuilder[Ss] = copy(distinct = true)

  // ---- Row-level locking (single-source Table only) ---------------------------------------------

  /** `FOR UPDATE` — exclusive row lock. Requires single-source Table; anything else is a compile error. */
  def forUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): SelectBuilder[Ss] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  // ---- Attach more sources (upgrade single-source → multi-source) -------------------------------

  /**
   * Attach another source via INNER JOIN. Transitions to [[IncompleteJoin]] — call `.on(...)` to finalise. The new
   * source's alias must not clash with any already-committed source alias (enforced by `AliasNotUsed`).
   */
  def innerJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, CR, AR] = {
    val rel  = a(next)
    val cols = rel.columns.asInstanceOf[CR]
    new IncompleteJoin[Ss, RR, CR, CR, AR](sources, rel, a.aliasValue(next), cols, cols, JoinKind.Inner)
  }

  /** Attach another source via LEFT JOIN. Right-side cols become nullable for subsequent `.where` / `.select`. */
  def leftJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, NullableCols[CR], AR] = {
    val rel          = a(next)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin[Ss, RR, CR, NullableCols[CR], AR](
      sources,
      rel,
      a.aliasValue(next),
      origCols,
      effectiveCls,
      JoinKind.Left
    )
  }

  /** Attach another source via CROSS JOIN. No `.on(...)` required. */
  def crossJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]] = {
    val rel   = a(next)
    val cols  = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](rel, a.aliasValue(next), cols, cols, JoinKind.Cross, None)
    val next2 = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]](
      next2,
      distinct,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  // ---- Projection -------------------------------------------------------------------------------

  /**
   * Projection — pick the columns / expressions to return. Single `TypedExpr[T]` → row is `T`; tuple (named or
   * positional) → row is the tuple of expression output types. `X` is threaded as the projection phantom (`Proj`) on
   * the resulting [[ProjectedSelect]] so downstream [[ProjectedSelect.groupBy]] / [[ProjectedSelect.compile]] can check
   * GROUP BY coverage.
   */
  transparent inline def select[X](inline f: SelectView[Ss] => X)
    : ProjectedSelect[Ss, NormProj[X], EmptyTuple, ProjResult[X]] = {
    val v = view
    f(v) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect[Ss, NormProj[X], EmptyTuple, ProjResult[X]](
          sources,
          distinct,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]],
          whereOpt,
          groupBys,
          havingOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt
        )
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedSelect[Ss, NormProj[X], EmptyTuple, ProjResult[X]](
          sources,
          distinct,
          exprs,
          codec,
          whereOpt,
          groupBys,
          havingOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt
        )
    }
  }

  transparent inline def apply[X](inline f: SelectView[Ss] => X)
    : ProjectedSelect[Ss, NormProj[X], EmptyTuple, ProjResult[X]] = select[X](f)

  /**
   * Whole-row `.compile` — only available on single-source builders. Multi-source builders must project first via
   * `.select(f)`.
   */
  def compile(using ev: IsSingleSource[Ss]): CompiledQuery[NamedRowOf[ev.Cols]] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head
    val cols    = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projStr = cols.map(c => s""""${c.name}"""").mkString(", ")
    val keyword = if (distinct) "SELECT DISTINCT " else "SELECT "
    val header  =
      if (head.relation.hasFromClause)
        TypedExpr.raw(s"$keyword$projStr FROM ") |+| aliasedFromEntry(head)
      else
        TypedExpr.raw(s"$keyword$projStr")
    val withClauses = renderClauses(header, whereOpt, groupBys, havingOpt, orderBys, limitOpt, offsetOpt, lockingOpt)
    CompiledQuery(withClauses, rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]])
  }

}

/**
 * A SELECT with an explicit projection list — rows have shape `Row` instead of the relation's default named tuple.
 * Shared between single-source and multi-source queries; the `Ss` parameter carries the full shape.
 *
 *   - `Proj` is the tuple-normalised projection (`X *: EmptyTuple` for single-expr, the tuple itself for multi-expr).
 *     Threaded from `.select[X](f)` so downstream `.groupBy` / `.compile` can check coverage.
 *   - `Groups` is the tuple of GROUP BY expressions; starts `EmptyTuple`, extended by `.groupBy[G]`.
 */
final class ProjectedSelect[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, Row](
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean,
  private[sharp] val projections: List[TypedExpr[?]],
  private[sharp] val codec: Codec[Row],
  private[sharp] val whereOpt: Option[Where],
  private[sharp] val groupBys: List[TypedExpr[?]],
  private[sharp] val havingOpt: Option[Where],
  private[sharp] val orderBys: List[OrderBy],
  private[sharp] val limitOpt: Option[Int],
  private[sharp] val offsetOpt: Option[Int],
  private[sharp] val lockingOpt: Option[Locking] = None
) {

  private def copy(
    distinct: Boolean = distinct,
    projections: List[TypedExpr[?]] = projections,
    codec: Codec[Row] = codec,
    whereOpt: Option[Where] = whereOpt,
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[Where] = havingOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt
  ): ProjectedSelect[Ss, Proj, Groups, Row] =
    new ProjectedSelect[Ss, Proj, Groups, Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )

  private def view: SelectView[Ss] = buildSelectView[Ss](sources)

  def where(f: SelectView[Ss] => Where): ProjectedSelect[Ss, Proj, Groups, Row] = {
    val next = whereOpt.fold(f(view))(_ && f(view))
    copy(whereOpt = Some(next))
  }

  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): ProjectedSelect[Ss, Proj, Groups, Row] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY …`. `G` is captured as a phantom — bare-column elements contribute their names to [[Groups]] for the
   * eventual coverage check at `.compile` time. Accumulates across multiple calls via `Tuple.Concat`.
   */
  transparent inline def groupBy[G](inline f: SelectView[Ss] => G)
    : ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], Row] = {
    val v     = view
    val fresh = f(v) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    new ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys ++ fresh,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  def having(f: SelectView[Ss] => Where): ProjectedSelect[Ss, Proj, Groups, Row] = {
    val next = havingOpt.fold(f(view))(_ && f(view))
    copy(havingOpt = Some(next))
  }

  def limit(n: Int): ProjectedSelect[Ss, Proj, Groups, Row]  = copy(limitOpt = Some(n))
  def offset(n: Int): ProjectedSelect[Ss, Proj, Groups, Row] = copy(offsetOpt = Some(n))

  def distinctRows: ProjectedSelect[Ss, Proj, Groups, Row] = copy(distinct = true)

  def forUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, Row] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  /**
   * Lift result rows into a case class `T`. Pure decoder transformation — SQL is unchanged. `Row` must be a tuple whose
   * element types align with `T`'s fields.
   */
  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = Row & Tuple }
  ): ProjectedSelect[Ss, Proj, Groups, T] = {
    val newCodec: Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[Row]
    )
    new ProjectedSelect[Ss, Proj, Groups, T](
      sources,
      distinct,
      projections,
      newCodec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  /**
   * Compile into an [[CompiledQuery]]. Enforces [[GroupCoverage]] — if GROUP BY is declared, every bare-column element
   * in the projection must appear in the GROUP BY; aggregates, literals, aliased expressions are free. When no GROUP BY
   * is declared, the check is vacuous.
   */
  def compile(using @scala.annotation.unused ev: GroupCoverage[Proj, Groups]): CompiledQuery[Row] = {
    val entries  = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val projList = TypedExpr.joined(projections.map(_.render), ", ")
    val keyword  = if (distinct) "SELECT DISTINCT " else "SELECT "
    val header   =
      if (entries.isEmpty || !entries.head.relation.hasFromClause)
        TypedExpr.raw(keyword) |+| projList
      else {
        val head     = entries.head
        val headFrag = TypedExpr.raw(keyword) |+| projList |+| TypedExpr.raw(" FROM ") |+| aliasedFromEntry(head)
        entries.tail.foldLeft(headFrag) { (acc, s) =>
          val fromFrag = TypedExpr.raw(s" ${s.kind.sql} ") |+| aliasedFromEntry(s)
          s.onPredOpt.fold(acc |+| fromFrag)(p => acc |+| fromFrag |+| TypedExpr.raw(" ON ") |+| p.render)
        }
      }
    val withClauses = renderClauses(header, whereOpt, groupBys, havingOpt, orderBys, limitOpt, offsetOpt, lockingOpt)
    CompiledQuery(withClauses, codec)
  }

}

// ---- Shared render helper ---------------------------------------------------------------------

private[dsl] def renderClauses(
  header: skunk.AppliedFragment,
  whereOpt: Option[Where],
  groupBys: List[TypedExpr[?]],
  havingOpt: Option[Where],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  lockingOpt: Option[Locking]
): skunk.AppliedFragment = {
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
  lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
}

// ---- Entry points -----------------------------------------------------------------------------

/**
 * `.select` on any relation-like value — bare `Table` / `View` (auto-aliased to its own name) or any relation
 * re-aliased via `.alias("…")`. Produces a single-source [[SelectBuilder]], from which `.where` / `.select(f)` /
 * `.innerJoin` etc. can be called.
 */
extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton, ML <: AliasMode](left: L)(using
  aL: AsRelation.Aux[L, RL, CL, AL, ML]
) {

  def select: SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple] = {
    val entry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    new SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple](entry *: EmptyTuple)
  }

}

/**
 * `empty.select(f)` — FROM-less SELECT, e.g. `empty.select(_ => Pg.now)` → `SELECT now()`. Lives separately from the
 * main `.select` extension because `empty` has no alias / Name, so it can't flow through the `AsRelation` machinery.
 */
extension (rel: skunk.sharp.empty.type) {

  transparent inline def select[X](inline f: ColumnsView[EmptyTuple] => X)
    : ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, ProjResult[X]] = {
    val v = ColumnsView(EmptyTuple)
    f(v) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, ProjResult[X]](
          EmptyTuple,
          false,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]],
          None,
          Nil,
          None,
          Nil,
          None,
          None,
          None
        )
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, ProjResult[X]](
          EmptyTuple,
          false,
          exprs,
          codec,
          None,
          Nil,
          None,
          Nil,
          None,
          None,
          None
        )
    }
  }

}

// ---- Match type: view receiver for lambdas -----------------------------------------------------

/**
 * The view type that `.where` / `.select` / `.orderBy` / `.groupBy` / `.having` receive. For a single source, this
 * reduces to `ColumnsView[Cols]` — the user writes `u.email`. For 2+ sources, it reduces to `JoinedView[Ss]` — the user
 * writes `r.users.email`.
 *
 * Pattern-matches the concrete `SourceEntry[?, ?, c, ?] *: EmptyTuple` shape directly in the first arm so the reduction
 * fires at call sites without needing extra evidence.
 */
type SelectView[Ss <: Tuple] = Ss match {
  case SourceEntry[?, ?, c, ?] *: EmptyTuple => ColumnsView[c]
  case _                                     => JoinedView[Ss]
}

private[sharp] def buildSelectView[Ss <: Tuple](sources: Ss): SelectView[Ss] =
  sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]] match {
    // Single source: columns render *bare* when the alias is the default (equal to the relation's name) — avoids
    // redundant qualifiers and matches the pre-unification SelectBuilder SQL. When the caller gave an explicit
    // `.alias(...)`, we qualify — it's how correlation inside subqueries works (outer `u.id` must render as
    // `"u"."id"` for Postgres to resolve to the outer source).
    case single :: Nil =>
      if (single.alias == single.relation.name)
        ColumnsView(single.effectiveCols).asInstanceOf[SelectView[Ss]]
      else
        ColumnsView.qualified(single.effectiveCols, single.alias).asInstanceOf[SelectView[Ss]]
    case _ => buildJoinedView[Ss](sources).asInstanceOf[SelectView[Ss]]
  }

// ---- Evidence typeclasses ----------------------------------------------------------------------

/**
 * Evidence that `Ss` is a single source anchored at a [[Table]] — the only shape where Postgres allows
 * `SELECT … FOR UPDATE` and the other row-level locks.
 */
sealed trait IsSingleTable[Ss]

object IsSingleTable {

  given [Cols <: Tuple, N <: String & Singleton, A <: String & Singleton]
    : IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A] *: EmptyTuple] =
    new IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A] *: EmptyTuple] {}

}

/**
 * Evidence that `Ss` has exactly one source, of any kind (Table or View). Required for the whole-row default `.compile`
 * — multi-source builders must project first.
 */
sealed trait IsSingleSource[Ss] {
  type Cols <: Tuple
}

object IsSingleSource {
  type Aux[Ss, C] = IsSingleSource[Ss] { type Cols = C }

  given [RR <: Relation[C], C <: Tuple, A <: String & Singleton]
    : IsSingleSource.Aux[SourceEntry[RR, C, C, A] *: EmptyTuple, C] =
    new IsSingleSource[SourceEntry[RR, C, C, A] *: EmptyTuple] { type Cols = C }

}

// ---- Locking enums + OrderBy + projection helpers ---------------------------------------------

/** Postgres row-level locking mode. */
enum LockMode(val sql: String) {
  case ForUpdate      extends LockMode("FOR UPDATE")
  case ForNoKeyUpdate extends LockMode("FOR NO KEY UPDATE")
  case ForShare       extends LockMode("FOR SHARE")
  case ForKeyShare    extends LockMode("FOR KEY SHARE")
}

/** Wait policy for a row-level lock. */
enum WaitPolicy(val sql: String) {
  case Wait       extends WaitPolicy("")
  case NoWait     extends WaitPolicy(" NOWAIT")
  case SkipLocked extends WaitPolicy(" SKIP LOCKED")
}

final case class Locking(mode: LockMode, waitPolicy: WaitPolicy = WaitPolicy.Wait) {
  def sql: String = mode.sql + waitPolicy.sql
}

/** Map a tuple of `TypedExpr[_]` to the tuple of their output types. */
type ExprOutputs[T <: Tuple] <: Tuple = T match {
  case EmptyTuple           => EmptyTuple
  case TypedExpr[t] *: tail => t *: ExprOutputs[tail]
}

/** Input-shape-aware projection result — single `TypedExpr[T]` → `T`, tuple → tuple of output types. */
type ProjResult[X] = X match {
  case TypedExpr[t]  => t
  case NonEmptyTuple => ExprOutputs[X & NonEmptyTuple]
}

/**
 * Normalise a projection / GROUP BY lambda return type into a tuple: a single `TypedExpr[_]` becomes a one-element
 * tuple; a tuple stays as-is. Feeds the [[GroupCoverage]] check uniformly regardless of single-vs-multi form.
 */
type NormProj[X] <: Tuple = X match {
  case NonEmptyTuple => X & Tuple
  case _             => X *: EmptyTuple
}

/** Type-level lookup: tuple of column names → tuple of value types. */
type LookupTypes[Cols <: Tuple, Names <: Tuple] <: Tuple = Names match {
  case EmptyTuple => EmptyTuple
  case n *: rest  => ColumnType[Cols, n & String & Singleton] *: LookupTypes[Cols, rest]
}

/**
 * ORDER BY clause — produced by `.asc` / `.desc` on a [[TypedColumn]]. Chain `.nullsFirst` / `.nullsLast` to control
 * where NULLs land.
 */
final case class OrderBy(sql: String) {
  def nullsFirst: OrderBy = OrderBy(sql + " NULLS FIRST")
  def nullsLast: OrderBy  = OrderBy(sql + " NULLS LAST")
}

extension [T, Null <: Boolean, N <: String & Singleton](col: TypedColumn[T, Null, N]) {
  def asc: OrderBy  = OrderBy(s"${col.sqlRef} ASC")
  def desc: OrderBy = OrderBy(s"${col.sqlRef} DESC")
}
