package skunk.sharp.dsl

import cats.Reducible
import skunk.{AppliedFragment, Codec, Encoder, Fragment}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, CompileChecks, RawConstants}
import skunk.util.Origin

import scala.NamedTuple
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

/**
 * INSERT builder. Threads `Args` end-to-end — for `INSERT … VALUES (…)`, `Args` is the captured row-values
 * tuple in user-listed column order. Batch and `INSERT … SELECT` widen to `?` (existential) because their
 * value tuples are runtime-shaped.
 *
 *   - `users.insert(row)` — single named-tuple row.
 *   - `users.insert.values(r1, r2, …)` / `.values(reducible)` — batch.
 *   - `.returning*` — Postgres `RETURNING`.
 *   - `.onConflict*` — upsert.
 */
final class InsertBuilder[Cols <: Tuple] private[sharp] (private[sharp] val table: Table[Cols, ?]) {

  /** Single-row insert from a named tuple. `Args = NamedTuple.DropNames[R]`. */
  inline def apply[R <: NamedTuple.AnyNamedTuple](
    row: R
  ): InsertCommand[Cols, NamedTuple.DropNames[R]] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val vs    = row.asInstanceOf[Tuple].toList
    val args  = Tuple.fromArray(vs.toArray[Any]).asInstanceOf[NamedTuple.DropNames[R]]
    InsertCommand.buildSingle[Cols, NamedTuple.DropNames[R]](table, names, args, OnConflict.None)
  }

  /** Batch from varargs. Args widened to `?` because the row count is runtime. */
  inline def values[R <: NamedTuple.AnyNamedTuple](row: R, more: R*): InsertCommand[Cols, ?] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rows  = (row :: more.toList).map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rows, OnConflict.None)
  }

  /** Batch from `Reducible`. */
  inline def values[F[_]: Reducible, R <: NamedTuple.AnyNamedTuple](
    rows: F[R]
  ): InsertCommand[Cols, ?] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rs, OnConflict.None)
  }

  /** Single-row insert from a case class. `Args = m.MirroredElemTypes`. */
  inline def apply[T <: Product](row: T)(using m: Mirror.ProductOf[T]): InsertCommand[Cols, m.MirroredElemTypes] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val vs    = row.productIterator.toList
    val args  = Tuple.fromArray(vs.toArray[Any]).asInstanceOf[m.MirroredElemTypes]
    InsertCommand.buildSingle[Cols, m.MirroredElemTypes](table, names, args, OnConflict.None)
  }

  /** Batch case-class variant. */
  inline def values[F[_]: Reducible, T <: Product](rows: F[T])(using m: Mirror.ProductOf[T]): InsertCommand[Cols, ?] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.productIterator.toList)
    InsertCommand.buildMany[Cols](table, names, rs, OnConflict.None)
  }

  /**
   * `INSERT INTO … SELECT …`. Args widened to `?` because the inner subquery's args are existential.
   */
  inline def from[Q, Row <: NamedTuple.AnyNamedTuple](src: Q)(using ev: AsSubquery[Q, Row]): InsertCommand[Cols, ?] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[Row], NamedTuple.DropNames[Row]]
    val names = constValueTuple[NamedTuple.Names[Row]].toList.asInstanceOf[List[String]]
    InsertCommand.buildFromQuery[Cols](table, names, ev.render(src), OnConflict.None)
  }

}

/** ON CONFLICT clause. */
sealed trait OnConflict

object OnConflict {

  case object None extends OnConflict
  case object DoNothing extends OnConflict
  final case class TargetDoNothing(columns: List[String]) extends OnConflict
  final case class TargetDoUpdate(columns: List[String], assignments: List[SetAssignment[?, ?]]) extends OnConflict
}

/**
 * The source of rows for an INSERT. Either a list of pre-encoded `VALUES` rows (each row's per-row
 * `AppliedFragment` is built once at construction time and stored verbatim — its args are baked) or a deferred
 * sub-query.
 */
sealed trait InsertSource

object InsertSource {

  /**
   * Single typed row — args visible as the row's value tuple. Fragment expects that tuple at apply time.
   */
  final case class TypedRow(fragment: Fragment[?], args: Any) extends InsertSource

  /**
   * Many rows — pre-applied as a list of `AppliedFragment`s; each row's args are baked into its own per-row
   * AppliedFragment. The combined SQL is `VALUES (…), (…)`. Args widened to `?` at the InsertCommand level.
   */
  final case class ManyRows(rows: List[AppliedFragment]) extends InsertSource

  /** `INSERT … SELECT` — the sub-query's render thunk. Args widened to `?`. */
  final case class FromQuery(render: () => AppliedFragment) extends InsertSource
}

final class InsertCommand[Cols <: Tuple, Args] private[sharp] (
  private[sharp] val table: Table[Cols, ?],
  private[sharp] val projected: List[Column[?, ?, ?, ?]],
  private[sharp] val source: InsertSource,
  private[sharp] val conflict: OnConflict
) {

  /** Compile into a [[CompiledCommand]]. */
  def compile: CompiledCommand[Args] = MutationAssembly.command[Args](insertParts)

  // ---- Body parts ---------------------------------------------------------------

  private def headerAf: AppliedFragment =
    if (projected.size == table.columns.size) table.insertIntoFullHeader
    else {
      val projections = projected.map(c => s""""${c.name}"""").mkString(", ")
      TypedExpr.raw(s"INSERT INTO ${table.qualifiedName} ($projections) ")
    }

  private def insertParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(headerAf))
    source match {
      case InsertSource.TypedRow(f, a) =>
        buf += Left(RawConstants.VALUES)
        buf += Right((f, a))
      case InsertSource.ManyRows(rows) =>
        buf += Left(TypedExpr.raw("VALUES "))
        buf += Left(TypedExpr.joined(rows, ", "))
      case InsertSource.FromQuery(thunk) =>
        buf += Left(thunk())
    }
    val cf = conflictFragment
    if (cf ne AppliedFragment.empty) buf += Left(cf)
    buf.toList
  }

  /** Append a `RETURNING <expr>` clause — single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val view = table.columnsView
    val expr = f(view)
    MutationAssembly.withReturning[Args, T](insertParts, List(expr), expr.codec)
  }

  /** Append a `RETURNING <e1>, <e2>, …` clause. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): CompiledQuery[Args, ExprOutputs[T]] = {
    val view  = table.columnsView
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, ExprOutputs[T]](insertParts, exprs, codec)
  }

  /** Append `RETURNING <all columns>`. */
  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Args, NamedRowOf[Cols]](insertParts, exprs, codec)
  }

  // ---- ON CONFLICT ----

  def onConflictDoNothing: InsertCommand[Cols, Args] = withConflict(OnConflict.DoNothing)

  def onConflict[T, Null <: Boolean, N <: String & Singleton](
    f: ColumnsView[Cols] => TypedColumn[T, Null, N]
  )(using
    ev: HasUniqueness[Cols, N] =:= true
  ): OnConflictBuilder[Cols, Args] = {
    val view = table.columnsView
    val col  = f(view)
    new OnConflictBuilder[Cols, Args](this, List(col.name))
  }

  def onConflictComposite[T <: NonEmptyTuple](
    f: ColumnsView[Cols] => T
  )(using
    ev: HasCompositeUniqueness[Cols, NamesOfTypedCols[T]] =:= true
  ): OnConflictBuilder[Cols, Args] = {
    val view  = table.columnsView
    val names = f(view).toList.asInstanceOf[List[TypedColumn[?, ?, ?]]].map(_.name)
    new OnConflictBuilder[Cols, Args](this, names)
  }

  private[sharp] def conflictFragment: AppliedFragment =
    conflict match {
      case OnConflict.None                  => AppliedFragment.empty
      case OnConflict.DoNothing             => TypedExpr.raw(" ON CONFLICT DO NOTHING")
      case OnConflict.TargetDoNothing(cols) =>
        TypedExpr.raw(s" ON CONFLICT (${cols.map(c => s""""$c"""").mkString(", ")}) DO NOTHING")
      case OnConflict.TargetDoUpdate(cols, sets) =>
        val header = TypedExpr.raw(s" ON CONFLICT (${cols.map(c => s""""$c"""").mkString(", ")}) DO UPDATE SET ")
        // Render assignments via their .fragment(args). Each SetAssignment[?, ?] is path-dependent so pull
        // out fragment+args here.
        header |+| TypedExpr.joined(
          sets.map(sa => sa.fragment.asInstanceOf[Fragment[Any]].apply(sa.args)),
          ", "
        )
    }

  private[sharp] def withConflict(c: OnConflict): InsertCommand[Cols, Args] =
    new InsertCommand[Cols, Args](table, projected, source, c)

  private[sharp] def tableColumns: Cols = table.columns
}

object InsertCommand {

  /** Single typed row — args are the user's value tuple. */
  private[sharp] def buildSingle[Cols <: Tuple, Args](
    table: Table[Cols, ?],
    names: List[String],
    args: Args,
    conflict: OnConflict
  ): InsertCommand[Cols, Args] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    val frag: Fragment[Args] = Fragment(List(Right(rowEnc.sql)), rowEnc.asInstanceOf[Encoder[Args]], Origin.unknown)
    new InsertCommand[Cols, Args](table, projected, InsertSource.TypedRow(frag, args), conflict)
  }

  /** Batch — pre-applies each row to its own AppliedFragment so all args are baked. */
  private[sharp] def buildMany[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    rows: List[List[Any]],
    conflict: OnConflict
  ): InsertCommand[Cols, Any] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    val rowFrag: Fragment[Tuple] =
      Fragment(parts = List(Right(rowEnc.sql)), encoder = rowEnc, origin = Origin.unknown)
    val applied = rows.map(r => rowFrag(Tuple.fromArray(r.toArray[Any])))
    new InsertCommand[Cols, Any](table, projected, InsertSource.ManyRows(applied), conflict)
  }

  private[sharp] def buildFromQuery[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    render: () => AppliedFragment,
    conflict: OnConflict
  ): InsertCommand[Cols, Any] =
    new InsertCommand[Cols, Any](table, lookupProjected(table, names), InsertSource.FromQuery(render), conflict)

  private def lookupProjected[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String]
  ): List[Column[?, ?, ?, ?]] = {
    val allCols = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    names.map(n =>
      allCols.find(_.name == n).getOrElse(
        sys.error(s"skunk-sharp: column $n passed compile check but not found at runtime in ${table.name}")
      )
    )
  }

}

/** Continuation after `.onConflict(col)`. */
final class OnConflictBuilder[Cols <: Tuple, Args] private[sharp] (
  cmd: InsertCommand[Cols, Args],
  cols: List[String]
) {

  def doNothing: InsertCommand[Cols, Args] = cmd.withConflict(OnConflict.TargetDoNothing(cols))

  def doUpdate(f: ColumnsView[Cols] => SetAssignment[?, ?] | Tuple): InsertCommand[Cols, Args] = {
    val view        = ColumnsView(cmd.tableColumns)
    val assignments = f(view) match {
      case sa: SetAssignment[?, ?] => List(sa)
      case t: Tuple                => t.toList.asInstanceOf[List[SetAssignment[?, ?]]]
    }
    cmd.withConflict(OnConflict.TargetDoUpdate(cols, assignments))
  }

  def doUpdateFromExcluded(
    f: (ColumnsView[Cols], ColumnsView[Cols]) => SetAssignment[?, ?] | Tuple
  ): InsertCommand[Cols, Args] = {
    val target      = ColumnsView(cmd.tableColumns)
    val excluded    = ColumnsView.qualifiedRaw(cmd.tableColumns, "excluded")
    val assignments = f(target, excluded) match {
      case sa: SetAssignment[?, ?] => List(sa)
      case t: Tuple                => t.toList.asInstanceOf[List[SetAssignment[?, ?]]]
    }
    cmd.withConflict(OnConflict.TargetDoUpdate(cols, assignments))
  }

}

/** INSERT entry point lives on [[Table]] (views reject at compile time). */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def insert: InsertBuilder[Cols] = new InsertBuilder[Cols](table)
}
