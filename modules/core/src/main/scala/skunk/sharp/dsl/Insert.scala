package skunk.sharp.dsl

import cats.Reducible
import skunk.{AppliedFragment, Codec, Fragment, Session}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, CompileChecks}
import skunk.util.Origin

import scala.NamedTuple
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

/**
 * INSERT builder.
 *
 *   - `users.insert(row)` — single-row insert. `row` is any named tuple whose fields are a subset of the table's
 *     columns; the subset must cover every required (non-defaulted) column. Columns marked via `.withDefault("id")`
 *     (sequence PKs, `now()`-backed timestamps, …) may be omitted and the database fills them in.
 *   - `users.insert.values(r1, r2, r3)` — batch insert; all rows share one `INSERT INTO … VALUES (…), (…)` statement
 *     and must share the same named-tuple shape.
 *   - `.returning(c => c.id)` / `.returningTuple(…)` / `.returningAll` — Postgres `RETURNING`.
 *   - `.onConflictDoNothing` / `.onConflict(c => c.id).doNothing` / `.doUpdate(…)` / `.doUpdateFromExcluded(…)` —
 *     upsert.
 */
final class InsertBuilder[Cols <: Tuple] private[sharp] (table: Table[Cols]) {

  /**
   * Insert a single row. `row` is any named tuple whose field names are a subset of the table's columns and whose
   * values have the column-declared Scala types. The subset must cover every required column (those without
   * `.withDefault`). Omitted defaulted columns are filled in by Postgres.
   */
  inline def apply[R <: NamedTuple.AnyNamedTuple](row: R): InsertCommand[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val vs    = row.asInstanceOf[Tuple].toList
    InsertCommand.build(table, names, List(vs), OnConflict.None)
  }

  /** Batch insert. All rows share the same named-tuple shape; picks the same subset of columns for every row. */
  inline def values[R <: NamedTuple.AnyNamedTuple](row: R, more: R*): InsertCommand[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rows  = (row :: more.toList).map(_.asInstanceOf[Tuple].toList)
    InsertCommand.build(table, names, rows, OnConflict.None)
  }

  /**
   * Bulk-from-container variant — works with any cats `Reducible` (`NonEmptyList`, `NonEmptyVector`, `NonEmptyChain`,
   * `NonEmptySeq`, …). `Reducible` is cats's typeclass for non-empty foldable structures, so the "at least one row"
   * guarantee is enforced at the type level; no runtime check needed.
   */
  inline def values[F[_]: Reducible, R <: NamedTuple.AnyNamedTuple](rows: F[R]): InsertCommand[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.asInstanceOf[Tuple].toList)
    InsertCommand.build(table, names, rs, OnConflict.None)
  }

  /**
   * Insert a single row from a case class instance. Uses `Mirror.ProductOf[T]` to read the case class's field labels
   * and types, runs the same subset/required/value-type compile-time checks as the named-tuple overload, and reads
   * values at runtime via `productIterator`. Case classes that omit `.withDefault`ed columns from their shape are fine
   * — the omitted columns are filled in by the database.
   */
  inline def apply[T <: Product](row: T)(using m: Mirror.ProductOf[T]): InsertCommand[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val vs    = row.productIterator.toList
    InsertCommand.build(table, names, List(vs), OnConflict.None)
  }

  /**
   * Batch case-class variant. Same contract as the named-tuple `.values`, but each row is a `T <: Product` instance
   * resolved via its shared `Mirror.ProductOf[T]`.
   */
  inline def values[F[_]: Reducible, T <: Product](rows: F[T])(using m: Mirror.ProductOf[T]): InsertCommand[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.productIterator.toList)
    InsertCommand.build(table, names, rs, OnConflict.None)
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
  private[sharp] val table: Table[Cols],
  private[sharp] val projected: List[Column[?, ?, ?, ?]],
  private[sharp] val rows: List[List[Any]],
  private[sharp] val conflict: OnConflict
) {

  /**
   * Compile into a skunk `AppliedFragment` ready to be executed as a command. Renders a multi-row `VALUES (…), (…)`
   * when more than one row is present, plus any `ON CONFLICT` clause.
   */
  def compile: AppliedFragment = {
    val projections              = projected.map(c => s""""${c.name}"""").mkString(", ")
    val perRow: Codec[Tuple]     = tupleCodec(projected.map(_.codec))
    val rowEnc                   = perRow.values
    val rowFrag: Fragment[Tuple] = Fragment(
      parts = List(Right(rowEnc.sql)),
      encoder = rowEnc,
      origin = Origin.unknown
    )
    val rowsApplied = rows.map(r => rowFrag(Tuple.fromArray(r.toArray[Any])))
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
    new InsertReturning[Cols, T](this, List(expr), expr.codec)
  }

  /** Append a `RETURNING <e1>, <e2>, …` clause — multi-value return. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): InsertReturning[Cols, ExprOutputs[T]] = {
    val view  = ColumnsView(table.columns)
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new InsertReturning[Cols, ExprOutputs[T]](this, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — the whole row, same shape as the table's default named-tuple projection. */
  def returningAll: InsertReturning[Cols, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]])
      )
    val codec = rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new InsertReturning[Cols, NamedRowOf[Cols]](this, exprs, codec)
  }

  // ---- ON CONFLICT ----

  /** `ON CONFLICT DO NOTHING` — no target, any conflict is silently skipped. */
  def onConflictDoNothing: InsertCommand[Cols] = withConflict(OnConflict.DoNothing)

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
    new InsertCommand[Cols](table, projected, rows, c)

  private[sharp] def tableColumns: Cols = table.columns
}

object InsertCommand {

  /**
   * Runtime builder used by [[InsertBuilder]]: looks up the projected columns by name from the table and constructs the
   * command. Names and value lists are parallel arrays — `names(i)` is the column name for `rows(r)(i)`.
   */
  private[sharp] def build[Cols <: Tuple](
    table: Table[Cols],
    names: List[String],
    rows: List[List[Any]],
    conflict: OnConflict
  ): InsertCommand[Cols] = {
    val allCols = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val proj    = names.map(n =>
      allCols.find(_.name == n).getOrElse(
        sys.error(s"skunk-sharp: column $n passed compile check but not found at runtime in ${table.name}")
      )
    )
    new InsertCommand[Cols](table, proj, rows, conflict)
  }

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
  cmd: InsertCommand[Cols],
  returning: List[TypedExpr[?]],
  returnCodec: Codec[R]
) {

  def compile: (AppliedFragment, Codec[R]) = {
    val returningList = TypedExpr.joined(returning.map(_.render), ", ")
    val applied       = cmd.compile |+| TypedExpr.raw(" RETURNING ") |+| returningList
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
