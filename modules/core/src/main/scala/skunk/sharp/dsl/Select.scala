package skunk.sharp.dsl

import cats.effect.MonadCancelThrow
import cats.syntax.all.*
import skunk.{AppliedFragment, Codec, Session}
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
final class SelectBuilder[Cols <: Tuple] private[sharp] (
  relation: Relation[Cols],
  distinct: Boolean,
  whereOpt: Option[Where],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  lockingOpt: Option[Locking]
) {

  private def copy(
    relation: Relation[Cols] = relation,
    distinct: Boolean = distinct,
    whereOpt: Option[Where] = whereOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt
  ): SelectBuilder[Cols] =
    new SelectBuilder[Cols](relation, distinct, whereOpt, orderBys, limitOpt, offsetOpt, lockingOpt)

  def where(f: ColumnsView[Cols] => Where): SelectBuilder[Cols] = {
    val view = ColumnsView(relation.columns)
    val next = whereOpt match {
      case Some(existing) => existing && f(view)
      case None           => f(view)
    }
    copy(whereOpt = Some(next))
  }

  def orderBy(f: ColumnsView[Cols] => OrderBy | Tuple): SelectBuilder[Cols] = {
    val view  = ColumnsView(relation.columns)
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    copy(orderBys = orderBys ++ fresh)
  }

  def limit(n: Int): SelectBuilder[Cols]  = copy(limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[Cols] = copy(offsetOpt = Some(n))

  /** Apply `SELECT DISTINCT …`. */
  def distinctRows: SelectBuilder[Cols] = copy(distinct = true)

  // ---- Postgres row-level locking (SELECT … FOR UPDATE etc.) ----

  /**
   * `FOR UPDATE` — exclusive row lock. Use `.noWait` / `.skipLocked` on the resulting builder to tweak the wait policy.
   */
  def forUpdate: SelectBuilder[Cols] = copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))

  /** `FOR NO KEY UPDATE` — exclusive but weaker; allows foreign-key checks to proceed. */
  def forNoKeyUpdate: SelectBuilder[Cols] = copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  /** `FOR SHARE` — shared row lock. */
  def forShare: SelectBuilder[Cols] = copy(lockingOpt = Some(Locking(LockMode.ForShare)))

  /** `FOR KEY SHARE` — weakest shared lock, blocks only DELETE and some UPDATEs. */
  def forKeyShare: SelectBuilder[Cols] = copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  /** Append ` SKIP LOCKED` — skip rows that are already locked (useful for queue-style consumers). */
  def skipLocked: SelectBuilder[Cols] =
    copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  /** Append ` NOWAIT` — fail immediately if any target row is already locked. */
  def noWait: SelectBuilder[Cols] =
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
  transparent inline def apply[X](inline f: ColumnsView[Cols] => X): ProjectedSelect[ProjResult[X]] = {
    val view = ColumnsView(relation.columns)
    f(view) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect(
          Some(relation),
          distinct,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]],
          whereOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt
        )
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedSelect(
          Some(relation),
          distinct,
          exprs,
          codec,
          whereOpt,
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

  /** Compile the default whole-row SELECT into an `AppliedFragment` plus row codec. */
  def compile: (AppliedFragment, Codec[ValuesOf[Cols]]) = {
    val cols        = relation.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projections = cols.map(c => s""""${c.name}"""").mkString(", ")
    val keyword     = if (distinct) "SELECT DISTINCT " else "SELECT "
    val sqlHeader   =
      if (relation.hasFromClause) s"$keyword$projections FROM ${relation.qualifiedName}"
      else s"$keyword$projections"
    val header    = TypedExpr.raw(sqlHeader)
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withOrder =
      if (orderBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit   = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset  = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    val withLocking = lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
    (withLocking, rowCodec(relation.columns))
  }

  def run[F[_]: MonadCancelThrow](session: Session[F]): F[List[NamedRowOf[Cols]]] = {
    val (af, codec) = compile
    val query       = af.fragment.query(codec)
    session.execute(query)(af.argument).map(_.asInstanceOf[List[NamedRowOf[Cols]]])
  }

}

// `relation.select` and `relation.*` were removed in favour of the top-level `select` entry point (see below).
// Mental model: start every query with `select` just like in SQL.

/**
 * A SELECT with an explicit projection list — rows have shape `R` instead of the relation's default named tuple. When
 * `relationOpt` is `None`, the query has no FROM clause (produced by the top-level [[select]] entry points).
 */
final class ProjectedSelect[R](
  relationOpt: Option[Relation[?]],
  distinct: Boolean,
  projections: List[TypedExpr[?]],
  codec: Codec[R],
  whereOpt: Option[Where],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  lockingOpt: Option[Locking] = None
) {

  private def copy(
    relationOpt: Option[Relation[?]] = relationOpt,
    distinct: Boolean = distinct,
    projections: List[TypedExpr[?]] = projections,
    codec: Codec[R] = codec,
    whereOpt: Option[Where] = whereOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt
  ): ProjectedSelect[R] =
    new ProjectedSelect[R](
      relationOpt,
      distinct,
      projections,
      codec,
      whereOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )

  def limit(n: Int): ProjectedSelect[R]  = copy(limitOpt = Some(n))
  def offset(n: Int): ProjectedSelect[R] = copy(offsetOpt = Some(n))

  /** `SELECT DISTINCT …`. */
  def distinctRows: ProjectedSelect[R] = copy(distinct = true)

  def forUpdate: ProjectedSelect[R]      = copy(lockingOpt = Some(Locking(LockMode.ForUpdate)))
  def forNoKeyUpdate: ProjectedSelect[R] = copy(lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))
  def forShare: ProjectedSelect[R]       = copy(lockingOpt = Some(Locking(LockMode.ForShare)))
  def forKeyShare: ProjectedSelect[R]    = copy(lockingOpt = Some(Locking(LockMode.ForKeyShare)))
  def skipLocked: ProjectedSelect[R]     = copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))
  def noWait: ProjectedSelect[R]         = copy(lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  /**
   * Map result rows into a case class `T`. `R` must already be a tuple whose element types line up with `T`'s fields
   * (verified at compile time via `Mirror.ProductOf`).
   */
  def as[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = R & Tuple }
  ): ProjectedSelect[T] = {
    val newCodec: Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[R]
    )
    new ProjectedSelect[T](
      relationOpt,
      distinct,
      projections,
      newCodec,
      whereOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  def compile: (AppliedFragment, Codec[R]) = {
    val projList = TypedExpr.joined(projections.map(_.render), ", ")
    val keyword  = if (distinct) "SELECT DISTINCT " else "SELECT "
    val header   =
      relationOpt.fold(TypedExpr.raw(keyword) |+| projList) { r =>
        if (r.hasFromClause)
          TypedExpr.raw(keyword) |+| projList |+| TypedExpr.raw(s" FROM ${r.qualifiedName}")
        else
          TypedExpr.raw(keyword) |+| projList
      }
    val withWhere = whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
    val withOrder =
      if (orderBys.isEmpty) withWhere
      else withWhere |+| TypedExpr.raw(" ORDER BY " + orderBys.map(_.sql).mkString(", "))
    val withLimit   = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
    val withOffset  = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
    val withLocking = lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
    (withLocking, codec)
  }

  def run[F[_]](session: Session[F]): F[List[R]] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.execute(query)(af.argument)
  }

  /** Run and return exactly one row. Fails if the row count is not 1. */
  def unique[F[_]](session: Session[F]): F[R] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.unique(query)(af.argument)
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
extension [Cols <: Tuple](relation: Relation[Cols]) {

  def select: SelectBuilder[Cols] =
    new SelectBuilder[Cols](relation, distinct = false, None, Nil, None, None, None)

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
