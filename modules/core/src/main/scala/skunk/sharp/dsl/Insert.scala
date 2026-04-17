package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Session}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec}
import skunk.util.Origin

/**
 * INSERT builder.
 *
 *   - `insert.into(users)(row)` — single-row insert.
 *   - `insert.into(users).values(r1, r2, r3)` / `.values(iterable)` — batch insert.
 *   - `.returning(c => c.id)` / `.returningTuple(…)` / `.returningAll` — Postgres `RETURNING`.
 *   - `.onConflictDoNothing` / `.onConflict(c => c.id).doNothing` — skip-on-conflict.
 *   - `.onConflict(c => c.id).doUpdate(c => (c.email := "new", c.age := 42))` — upsert.
 *
 * Future (v0.1+): subset named tuple (omit defaulted columns automatically), `excluded.<col>` references inside
 * `DO UPDATE SET` right-hand sides (Postgres's way of referencing the incoming row in the conflict resolution).
 */
final class InsertBuilder[Cols <: Tuple] private[sharp] (table: Table[Cols]) {

  /** Insert a single row. `row` is a named tuple matching the table's declared shape. */
  def apply(row: NamedRowOf[Cols]): InsertCommand[Cols] =
    InsertCommand(table, List(row), OnConflict.None)

  /** Batch insert. All rows share one `INSERT INTO … VALUES (…), (…)` statement. */
  def values(row: NamedRowOf[Cols], more: NamedRowOf[Cols]*): InsertCommand[Cols] =
    InsertCommand(table, row :: more.toList, OnConflict.None)

  /** Bulk-from-collection variant. Empty input is rejected at runtime. */
  def values(rows: Iterable[NamedRowOf[Cols]]): InsertCommand[Cols] = {
    require(rows.nonEmpty, "insert.values needs at least one row")
    InsertCommand(table, rows.toList, OnConflict.None)
  }

}

/** ON CONFLICT clause. */
sealed trait OnConflict

object OnConflict {

  /** No ON CONFLICT clause — violations are raised as usual. */
  case object None extends OnConflict

  /** `ON CONFLICT DO NOTHING` (no target — any unique/exclusion violation is silently skipped). */
  case object DoNothing extends OnConflict

  /** `ON CONFLICT (col1, col2, …) DO NOTHING`. */
  final case class TargetDoNothing(columns: List[String]) extends OnConflict

  /** `ON CONFLICT (col1, col2, …) DO UPDATE SET …`. */
  final case class TargetDoUpdate(columns: List[String], assignments: List[SetAssignment[?]]) extends OnConflict
}

final class InsertCommand[Cols <: Tuple] private[sharp] (
  table: Table[Cols],
  rows: List[NamedRowOf[Cols]],
  conflict: OnConflict
) {

  /**
   * Compile into a skunk `AppliedFragment` ready to be executed as a command. Renders a multi-row `VALUES (…), (…)`
   * when more than one row is present, plus any `ON CONFLICT` clause.
   */
  def compile: AppliedFragment = {
    val cols                              = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projections                       = cols.map(c => s""""${c.name}"""").mkString(", ")
    val perRow                            = rowCodec(table.columns)
    val rowFrag: Fragment[ValuesOf[Cols]] = Fragment(
      parts = List(Left("("), Right(perRow.sql), Left(")")),
      encoder = perRow,
      origin = Origin.unknown
    )
    val rowsApplied = rows.map(r => rowFrag(r.asInstanceOf[ValuesOf[Cols]]))
    val header      = TypedExpr.raw(s"INSERT INTO ${table.qualifiedName} ($projections) VALUES ")
    val withValues  = header |+| TypedExpr.joined(rowsApplied, ", ")
    withValues |+| conflictFragment
  }

  /** Execute against a skunk session. Returns the completion message. */
  def run[F[_]](session: Session[F]): F[skunk.data.Completion] = {
    val af  = compile
    val cmd = af.fragment.command
    session.execute(cmd)(af.argument)
  }

  /** Append a `RETURNING <expr>` clause. Single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): InsertReturning[Cols, T] = {
    val view = ColumnsView(table.columns)
    val expr = f(view)
    new InsertReturning[Cols, T](table, rows, conflict, List(expr), expr.codec)
  }

  /** Append a `RETURNING <e1>, <e2>, …` clause — multi-value return. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): InsertReturning[Cols, ExprOutputs[T]] = {
    val view  = ColumnsView(table.columns)
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new InsertReturning[Cols, ExprOutputs[T]](table, rows, conflict, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — the whole row, same shape as the table's default named-tuple projection. */
  def returningAll: InsertReturning[Cols, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]])
      )
    val codec = rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new InsertReturning[Cols, NamedRowOf[Cols]](table, rows, conflict, exprs, codec)
  }

  // ---- ON CONFLICT ----

  /** `ON CONFLICT DO NOTHING` — no target, any conflict is silently skipped. */
  def onConflictDoNothing: InsertCommand[Cols] =
    new InsertCommand[Cols](table, rows, OnConflict.DoNothing)

  /**
   * Start a targeted `ON CONFLICT (col …) …` clause. Pick columns via the usual lambda. Pass a single column or a tuple
   * of columns.
   */
  def onConflict(f: ColumnsView[Cols] => TypedColumn[?, ?] | Tuple): OnConflictBuilder[Cols] = {
    val view  = ColumnsView(table.columns)
    val names = f(view) match {
      case c: TypedColumn[?, ?] => List(c.name)
      case t: Tuple             => t.toList.asInstanceOf[List[TypedColumn[?, ?]]].map(_.name)
    }
    new OnConflictBuilder[Cols](this, names)
  }

  private[sharp] def conflictFragment: AppliedFragment =
    conflict match {
      case OnConflict.None                  => AppliedFragment.empty
      case OnConflict.DoNothing             => TypedExpr.raw(" ON CONFLICT DO NOTHING")
      case OnConflict.TargetDoNothing(cols) =>
        TypedExpr.raw(s" ON CONFLICT (${cols.map(c => s""""$c"""").mkString(", ")}) DO NOTHING")
      case OnConflict.TargetDoUpdate(cols, sets) =>
        val header = TypedExpr.raw(s" ON CONFLICT (${cols.map(c => s""""$c"""").mkString(", ")}) DO UPDATE SET ")
        header |+| TypedExpr.joined(sets.map(_.render), ", ")
    }

  private[sharp] def withConflict(c: OnConflict): InsertCommand[Cols] =
    new InsertCommand[Cols](table, rows, c)

  private[sharp] def tableColumns: Cols = table.columns
}

/** Continuation after `.onConflict(col)` — choose `.doNothing` or `.doUpdate(…)`. */
final class OnConflictBuilder[Cols <: Tuple] private[sharp] (cmd: InsertCommand[Cols], cols: List[String]) {

  def doNothing: InsertCommand[Cols] = cmd.withConflict(OnConflict.TargetDoNothing(cols))

  /**
   * `DO UPDATE SET …`. The lambda receives just the target columns — use literal values or fully-qualified expressions
   * on the RHS. For the `excluded.<col>` form (referencing the incoming row), use [[doUpdateFromExcluded]] which passes
   * both the target view and the `excluded` view.
   */
  def doUpdate(f: ColumnsView[Cols] => SetAssignment[?] | Tuple): InsertCommand[Cols] = {
    val view        = ColumnsView(cmd.tableColumns)
    val assignments = f(view) match {
      case sa: SetAssignment[?] => List(sa)
      case t: Tuple             => t.toList.asInstanceOf[List[SetAssignment[?]]]
    }
    cmd.withConflict(OnConflict.TargetDoUpdate(cols, assignments))
  }

  /**
   * `DO UPDATE SET …` with access to Postgres's `excluded.<col>` pseudo-table (the incoming row). The lambda receives
   * `(target, excluded)` — e.g. `(t, ex) => t.email := ex.email`.
   */
  def doUpdateFromExcluded(
    f: (ColumnsView[Cols], ColumnsView[Cols]) => SetAssignment[?] | Tuple
  ): InsertCommand[Cols] = {
    val target      = ColumnsView(cmd.tableColumns)
    val excluded    = ColumnsView.qualified(cmd.tableColumns, "excluded")
    val assignments = f(target, excluded) match {
      case sa: SetAssignment[?] => List(sa)
      case t: Tuple             => t.toList.asInstanceOf[List[SetAssignment[?]]]
    }
    cmd.withConflict(OnConflict.TargetDoUpdate(cols, assignments))
  }

}

/** INSERT with a `RETURNING` clause. `R` is the returned row shape (single value or tuple). */
final class InsertReturning[Cols <: Tuple, R] private[sharp] (
  table: Table[Cols],
  rows: List[NamedRowOf[Cols]],
  conflict: OnConflict,
  returning: List[TypedExpr[?]],
  returnCodec: Codec[R]
) {

  def compile: (AppliedFragment, Codec[R]) = {
    val cols                              = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projections                       = cols.map(c => s""""${c.name}"""").mkString(", ")
    val perRow                            = rowCodec(table.columns)
    val returningList                     = TypedExpr.joined(returning.map(_.render), ", ")
    val rowFrag: Fragment[ValuesOf[Cols]] = Fragment(
      parts = List(Left("("), Right(perRow.sql), Left(")")),
      encoder = perRow,
      origin = Origin.unknown
    )
    val rowsApplied  = rows.map(r => rowFrag(r.asInstanceOf[ValuesOf[Cols]]))
    val header       = TypedExpr.raw(s"INSERT INTO ${table.qualifiedName} ($projections) VALUES ")
    val conflictFrag = new InsertCommand[Cols](table, rows, conflict).conflictFragment
    val applied      =
      header |+| TypedExpr.joined(rowsApplied, ", ") |+| conflictFrag |+|
        TypedExpr.raw(" RETURNING ") |+| returningList
    (applied, returnCodec)
  }

  def run[F[_]](session: Session[F]): F[List[R]] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.execute(query)(af.argument)
  }

  /** Run and return exactly one row (the single inserted row). */
  def unique[F[_]](session: Session[F]): F[R] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.unique(query)(af.argument)
  }

}

/** INSERT entry point lives on [[Table]] (views reject at compile time). */
extension [Cols <: Tuple](table: Table[Cols]) {
  def insert: InsertBuilder[Cols] = new InsertBuilder[Cols](table)
}
