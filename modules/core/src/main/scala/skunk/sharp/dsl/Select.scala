package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec}
import skunk.sharp.where.Where

/**
 * SELECT builder — anchored at a single relation.
 *
 *   - Default projection: the whole row as a Scala 3 named tuple (`NamedRowOf[Cols]`).
 *   - `.apply(cols => …)` — **function-based** projection. See trade-offs below.
 *   - `.cols(tupleOfNames)` — **name-based** projection. See trade-offs below.
 *   - `.where(cols => …)`, `.orderBy(cols => c.x.desc)`, `.limit(n)`, `.offset(n)`.
 *   - `.run(session)` materialises the query as a skunk `AppliedFragment` and executes it.
 *
 * ==Function-based vs name-based projection==
 *
 * ''TODO: carry these trade-offs into the docs module when Laika+mdoc land.''
 *
 *   - `.apply(cols => …)` (function form):
 *     - Expressions are first class — `Pg.lower(cols.email)`, arithmetic, user functions, column-to-column composition
 *       all compose naturally.
 *     - IDE autocomplete on `cols.` surfaces every column; refactoring rename-column updates references.
 *     - Scope is clear: the lambda binds `cols`, making it obvious which columns are visible.
 *     - Slightly more ceremony for simple projections (`cols =>` at the head).
 *   - `.cols(("id", "email"))` (name form):
 *     - Terse for straight "select these named columns" queries.
 *     - Unknown column names produce a compile error that lists the available columns.
 *     - No expression support — pure column references only.
 *     - Not refactor-safe: renaming a column won't update string literals (the compile-time check catches drift, but
 *       you still have to change every call site by hand).
 *     - Reads closer to SQL for one-off projections.
 *
 * For FROM-less queries (`SELECT now()`), use the top-level [[skunk.sharp.dsl.Select]] entry point instead. Multi-table
 * joins are roadmap (v0.1+).
 */
final class SelectBuilder[R <: Relation[Cols], Cols <: Tuple] private[sharp] (
  private[sharp] val relation: R,
  private[sharp] val distinct: Boolean,
  private[sharp] val whereOpt: Option[Where],
  private[sharp] val groupBys: List[TypedExpr[?]],
  private[sharp] val havingOpt: Option[Where],
  private[sharp] val orderBys: List[OrderBy],
  private[sharp] val limitOpt: Option[Int],
  private[sharp] val offsetOpt: Option[Int],
  private[sharp] val lockingOpt: Option[Locking]
) {

  private def copy(
    relation: R = relation,
    distinct: Boolean = distinct,
    whereOpt: Option[Where] = whereOpt,
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[Where] = havingOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt
  ): SelectBuilder[R, Cols] =
    new SelectBuilder[R, Cols](
      relation,
      distinct,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )

  def where(f: ColumnsView[Cols] => Where): SelectBuilder[R, Cols] = {
    val view = ColumnsView(relation.columns)
    val next = whereOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(whereOpt = Some(next))
  }

  def orderBy(f: ColumnsView[Cols] => OrderBy | Tuple): SelectBuilder[R, Cols] = {
    val view  = ColumnsView(relation.columns)
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY expr1, expr2, …`. Pass a single expression or a tuple. Does **not** validate at compile time that every
   * bare column in the SELECT is covered — Postgres raises that as a loud runtime error. Aggregates in `Pg.count`,
   * `Pg.sum`, etc. do not need to appear here.
   */
  def groupBy(f: ColumnsView[Cols] => TypedExpr[?] | Tuple): SelectBuilder[R, Cols] = {
    val view  = ColumnsView(relation.columns)
    val fresh = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    copy(groupBys = groupBys ++ fresh)
  }

  /** `HAVING <predicate>`. Chains with AND if called multiple times. Typically references aggregates. */
  def having(f: ColumnsView[Cols] => Where): SelectBuilder[R, Cols] = {
    val view = ColumnsView(relation.columns)
    val next = havingOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(havingOpt = Some(next))
  }

  def limit(n: Int): SelectBuilder[R, Cols]  = copy(limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[R, Cols] = copy(offsetOpt = Some(n))

  /** Apply `SELECT DISTINCT …`. */
  def distinctRows: SelectBuilder[R, Cols] = copy(distinct = true)

  // ---- Postgres row-level locking (SELECT … FOR UPDATE etc.) ----
  // Gated on `R <:< Table[Cols]` — `SELECT … FOR UPDATE` against a view is a Postgres error.

  /**
   * `FOR UPDATE` — exclusive row lock. Use `.noWait` / `.skipLocked` on the resulting builder to tweak the wait policy.
   * Only available when the underlying relation is a [[Table]]: views reject at compile time.
   */
  def forUpdate(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))

  /** `FOR NO KEY UPDATE` — exclusive but weaker; allows foreign-key checks to proceed. */
  def forNoKeyUpdate(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  /** `FOR SHARE` — shared row lock. */
  def forShare(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = Some(Locking(LockMode.ForShare)))

  /** `FOR KEY SHARE` — weakest shared lock, blocks only DELETE and some UPDATEs. */
  def forKeyShare(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  /** Append ` SKIP LOCKED` — skip rows that are already locked (useful for queue-style consumers). */
  def skipLocked(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  /** Append ` NOWAIT` — fail immediately if any target row is already locked. */
  def noWait(using ev: R <:< Table[Cols]): SelectBuilder[R, Cols] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  /**
   * Projection. Accepts either a single `TypedExpr[T]` (row shape is `T`) or a non-empty tuple of `TypedExpr`s (row
   * shape is a tuple of the expressions' output types).
   *
   * {{{
   *   select.from(users)(u => u.email)                // ProjectedSelect[String]
   *   select.from(users)(u => Pg.lower(u.email))      // ProjectedSelect[String]
   *   select.from(users)(u => (u.email, u.age))       // ProjectedSelect[(String, Int)]
   * }}}
   */
  transparent inline def apply[X](inline f: ColumnsView[Cols] => X): ProjectedSelect[R, Cols, ProjResult[X]] = {
    val view = ColumnsView(relation.columns)
    f(view) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect[R, Cols, ProjResult[X]](
          relation,
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
        new ProjectedSelect[R, Cols, ProjResult[X]](
          relation,
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

  // `.cols(("id", "email"))` name-based projection lives on the roadmap (v0.1+). Passing a regular tuple literal
  // widens its elements to `String`, so the singleton information needed to look up `ColumnType[Cols, "id"]` is lost
  // at the call site. Options to revisit: (a) a quoted macro that reads the literal string values at compile time;
  // (b) an explicit type parameter — `.cols[("id","email")](("id","email"))`; (c) wait for twiddles-backed tuple
  // ergonomics. Until then, use the function form: `select.from(users)(u => (u.id, u.email))`.

  /**
   * Compile the default whole-row SELECT into a [[CompiledQuery]]. Use the extensions in [[Compiled]] to execute it.
   */
  def compile: CompiledQuery[NamedRowOf[Cols]] = {
    val cols        = relation.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projections = cols.map(c => s""""${c.name}"""").mkString(", ")
    val keyword     = if (distinct) "SELECT DISTINCT " else "SELECT "
    val sqlHeader   =
      if (relation.hasFromClause) s"$keyword$projections FROM ${relation.qualifiedName}"
      else s"$keyword$projections"
    val header    = TypedExpr.raw(sqlHeader)
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withGroup =
      if (groupBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" GROUP BY ") |+| TypedExpr.joined(groupBys.map(_.render), ", ")
    val withHaving = havingOpt.fold(withGroup)(h => withGroup |+| TypedExpr.raw(" HAVING ") |+| h.render)
    val withOrder  =
      if (orderBys.isEmpty) withHaving
      else withHaving |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit   = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset  = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    val withLocking = lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
    CompiledQuery(withLocking, rowCodec(relation.columns).asInstanceOf[Codec[NamedRowOf[Cols]]])
  }

}

/**
 * A SELECT with an explicit projection list — rows have shape `Row` instead of the relation's default named tuple. The
 * relation type parameter `R` is carried through so locking methods (`.forUpdate`, `.forShare`, …) remain gated to
 * `Table` (rejected at compile time on a `View`).
 */
final class ProjectedSelect[R <: Relation[Cols], Cols <: Tuple, Row](
  relation: R,
  distinct: Boolean,
  projections: List[TypedExpr[?]],
  codec: Codec[Row],
  whereOpt: Option[Where],
  groupBys: List[TypedExpr[?]],
  havingOpt: Option[Where],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  lockingOpt: Option[Locking] = None
) {

  private def copy(
    relation: R = relation,
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
  ): ProjectedSelect[R, Cols, Row] =
    new ProjectedSelect[R, Cols, Row](
      relation,
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

  def limit(n: Int): ProjectedSelect[R, Cols, Row]  = copy(limitOpt = Some(n))
  def offset(n: Int): ProjectedSelect[R, Cols, Row] = copy(offsetOpt = Some(n))

  /** `SELECT DISTINCT …`. */
  def distinctRows: ProjectedSelect[R, Cols, Row] = copy(distinct = true)

  /** `GROUP BY expr1, expr2, …`. See [[SelectBuilder.groupBy]] for details. */
  def groupBy(f: ColumnsView[Cols] => TypedExpr[?] | Tuple): ProjectedSelect[R, Cols, Row] = {
    val view  = ColumnsView(relation.columns)
    val fresh = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    copy(groupBys = groupBys ++ fresh)
  }

  /** `HAVING <predicate>`. Chains with AND if called multiple times. */
  def having(f: ColumnsView[Cols] => Where): ProjectedSelect[R, Cols, Row] = {
    val view = ColumnsView(relation.columns)
    val next = havingOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(havingOpt = Some(next))
  }

  // ---- Row-level locking: gated on R <:< Table[Cols]. ----
  def forUpdate(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: R <:< Table[Cols]): ProjectedSelect[R, Cols, Row] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  /**
   * Map result rows into a case class `T`. `Row` must already be a tuple whose element types line up with `T`'s fields
   * (verified at compile time via `Mirror.ProductOf`).
   */
  def as[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = Row & Tuple }
  ): ProjectedSelect[R, Cols, T] = {
    val newCodec: Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[Row]
    )
    new ProjectedSelect[R, Cols, T](
      relation,
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

  def compile: CompiledQuery[Row] = {
    val projList = TypedExpr.joined(projections.map(_.render), ", ")
    val keyword  = if (distinct) "SELECT DISTINCT " else "SELECT "
    val header   =
      if (relation.hasFromClause)
        TypedExpr.raw(keyword) |+| projList |+| TypedExpr.raw(s" FROM ${relation.qualifiedName}")
      else
        TypedExpr.raw(keyword) |+| projList
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withGroup =
      if (groupBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" GROUP BY ") |+| TypedExpr.joined(groupBys.map(_.render), ", ")
    val withHaving = havingOpt.fold(withGroup)(h => withGroup |+| TypedExpr.raw(" HAVING ") |+| h.render)
    val withOrder  =
      if (orderBys.isEmpty) withHaving
      else withHaving |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit   = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset  = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    val withLocking = lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
    CompiledQuery(withLocking, codec)
  }

}

/**
 * Start a SELECT query: `users.select`. Extension lives on [[Relation]], so both [[Table]] and [[View]] work.
 *
 *   - `users.select` → `SelectBuilder[Cols]` (whole row); chain `.where`, `.orderBy`, `.limit`, `.offset`,
 *     `.distinctRows`, `.forUpdate` / `.forShare` / `.forNoKeyUpdate` / `.forKeyShare` (+ `.skipLocked` / `.noWait`),
 *     or call `.apply(u => …)` to narrow to a projection.
 *   - `empty.select(_ => Pg.now)` → FROM-less query via the dedicated `empty` relation.
 *
 * Multi-table JOINs will need a dedicated entry (roadmap).
 */
extension [R <: Relation[Cols], Cols <: Tuple](relation: R) {

  def select: SelectBuilder[R, Cols] =
    new SelectBuilder[R, Cols](
      relation,
      distinct = false,
      whereOpt = None,
      groupBys = Nil,
      havingOpt = None,
      orderBys = Nil,
      limitOpt = None,
      offsetOpt = None,
      lockingOpt = None
    )

}

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

/**
 * Input-shape-aware projection result.
 *
 *   - Single `TypedExpr[T]` → `T`.
 *   - Tuple (named or plain) of `TypedExpr`s → plain tuple of their output types. Named-tuple labels are NOT carried
 *     into the result type in this release (Scala 3.8's match-type soundness check rejects the natural pattern). If you
 *     need a named result, pipe through `.as[MyCaseClass]` or wrap at the call site.
 */
type ProjResult[X] = X match {
  case TypedExpr[t]  => t
  case NonEmptyTuple => ExprOutputs[X & NonEmptyTuple]
}

/** Type-level lookup: for a tuple of column names `Names`, the tuple of their Scala value types from `Cols`. */
type LookupTypes[Cols <: Tuple, Names <: Tuple] <: Tuple = Names match {
  case EmptyTuple => EmptyTuple
  case n *: rest  => ColumnType[Cols, n & String & Singleton] *: LookupTypes[Cols, rest]
}

/** ORDER BY clause. Produced by `.asc` / `.desc` on a [[TypedColumn]]. */
final case class OrderBy(sql: String)

extension [T, Null <: Boolean](col: TypedColumn[T, Null]) {
  def asc: OrderBy  = OrderBy(s""""${col.name}" ASC""")
  def desc: OrderBy = OrderBy(s""""${col.name}" DESC""")
}
