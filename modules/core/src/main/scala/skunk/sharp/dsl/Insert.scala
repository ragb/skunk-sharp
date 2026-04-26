package skunk.sharp.dsl

import cats.Reducible
import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, CompileChecks, RawConstants}
import skunk.util.Origin

import scala.NamedTuple
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

/**
 * INSERT builder.
 *
 *   - `users.insert((email = "x", age = 18))` (named tuple) / `users.insert(caseClassInstance)` — single-row,
 *     values baked via [[Param.bind]], `Args = Void`.
 *   - `users.insert.values(row, more*)` / `users.insert.values(reducible)` — batch, values baked, `Args = Void`.
 *   - `users.insert.from(query)` — `INSERT … SELECT`, `Args` threads from the inner subquery.
 *
 * The typed projection-then-values flow (`.into(u => (u.email, u.age)).values((email = Param[String], …))`)
 * for `CommandTemplate[(String, Int)]`-shaped templates is not yet shipped: it requires a per-field Args
 * reduction typeclass to make the result Args concrete (otherwise `.values(typed)` returns `Args = ?` and
 * users can't ascribe a typed template, defeating the purpose). Tracked as roadmap; until then, use the
 * SELECT-side `Param[T]` story for prepared-template re-binding and bake INSERT values directly.
 */
final class InsertBuilder[Cols <: Tuple] private[sharp] (private[sharp] val table: Table[Cols, ?]) {

  /** Single-row insert from a named tuple of values. Args = Void (values baked via Param.bind). */
  inline def apply[R <: NamedTuple.AnyNamedTuple](row: R): InsertCommand[Cols, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val vs    = row.asInstanceOf[Tuple].toList
    InsertCommand.buildSingleBaked[Cols](table, names, vs, OnConflict.None)
  }

  /** Single-row insert from a case-class instance. Args = Void. */
  inline def apply[T <: Product](row: T)(using m: Mirror.ProductOf[T]): InsertCommand[Cols, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val vs    = row.productIterator.toList
    InsertCommand.buildSingleBaked[Cols](table, names, vs, OnConflict.None)
  }

  /** Batch from varargs. Values baked; Args = Void. */
  inline def values[R <: NamedTuple.AnyNamedTuple](row: R, more: R*): InsertCommand[Cols, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rows  = (row :: more.toList).map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rows, OnConflict.None)
  }

  /** Batch from `Reducible`. Args = Void. */
  inline def values[F[_]: Reducible, R <: NamedTuple.AnyNamedTuple](rows: F[R]): InsertCommand[Cols, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rs, OnConflict.None)
  }

  inline def values[F[_]: Reducible, T <: Product](rows: F[T])(using m: Mirror.ProductOf[T]): InsertCommand[Cols, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.productIterator.toList)
    InsertCommand.buildMany[Cols](table, names, rs, OnConflict.None)
  }

  /** `INSERT INTO … SELECT …`. The inner subquery's `Args` threads via the `AsSubquery` evidence. */
  inline def from[Q, Row <: NamedTuple.AnyNamedTuple, Args](src: Q)(using
    ev: AsSubquery[Q, Row, Args]
  ): InsertCommand[Cols, Args] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[Row], NamedTuple.DropNames[Row]]
    val names = constValueTuple[NamedTuple.Names[Row]].toList.asInstanceOf[List[String]]
    InsertCommand.buildFromQuery[Cols, Args](table, names, ev.fragment(src), OnConflict.None)
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
 * The source of rows for an INSERT. Either a typed row fragment (single or batch) or a deferred sub-query.
 */
sealed trait InsertSource

object InsertSource {

  /** A single typed row fragment — `(<col-tuple-encoder-shape>)`. Encoder may carry typed Args or be Void-baked. */
  final case class TypedRow(fragment: Fragment[?]) extends InsertSource

  /** Many rows pre-applied: each row's args are baked. Combined SQL is `VALUES (…), (…)`. */
  final case class ManyRows(rows: List[AppliedFragment]) extends InsertSource

  /** `INSERT … SELECT` — the sub-query's typed fragment. */
  final case class FromQuery(fragment: Fragment[?]) extends InsertSource
}

final class InsertCommand[Cols <: Tuple, Args] private[sharp] (
  private[sharp] val table: Table[Cols, ?],
  private[sharp] val projected: List[Column[?, ?, ?, ?]],
  private[sharp] val source: InsertSource,
  private[sharp] val conflict: OnConflict
) {

  def compile: CommandTemplate[Args] = MutationAssembly.command[Args](insertParts)

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
      case InsertSource.TypedRow(f) =>
        // f's encoder bakes the values via contramap (Args = Void); apply to lift to AppliedFragment.
        buf += Left(RawConstants.VALUES)
        buf += Left(f.asInstanceOf[Fragment[Void]].apply(Void))
      case InsertSource.ManyRows(rows) =>
        buf += Left(TypedExpr.raw("VALUES "))
        buf += Left(TypedExpr.joined(rows, ", "))
      case InsertSource.FromQuery(frag) =>
        // INSERT … SELECT — subquery may carry typed Args.
        buf += Right(frag)
    }
    val cf = conflictFragment
    if (cf ne AppliedFragment.empty) buf += Left(cf)
    buf.toList
  }

  def returning[T, A](f: ColumnsView[Cols] => TypedExpr[T, A]): QueryTemplate[Args, T] = {
    val view = table.columnsView
    val expr = f(view)
    MutationAssembly.withReturning[Args, T](insertParts, List(expr), expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): QueryTemplate[Args, ExprOutputs[T]] = {
    val view  = table.columnsView
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, ExprOutputs[T]](insertParts, exprs, codec)
  }

  def returningAll: QueryTemplate[Args, NamedRowOf[Cols]] = {
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
        // Each SetAssignment[?, ?]'s fragment may carry typed Args; for the AppliedFragment path here we
        // bind Void (assumes value-baked path — ON CONFLICT DO UPDATE typically uses the EXCLUDED pseudo-table
        // or constant values). Typed-args threading through ON CONFLICT is roadmap.
        header |+| TypedExpr.joined(
          sets.map(sa => sa.fragment.asInstanceOf[Fragment[Void]].apply(Void)),
          ", "
        )
    }

  private[sharp] def withConflict(c: OnConflict): InsertCommand[Cols, Args] =
    new InsertCommand[Cols, Args](table, projected, source, c)

  private[sharp] def tableColumns: Cols = table.columns
}

object InsertCommand {

  /** Build a single-row insert with values baked via Param.bind. Args = Void. */
  private[sharp] def buildSingleBaked[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    values: List[Any],
    conflict: OnConflict
  ): InsertCommand[Cols, Void] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    // Bake values via contramap so the resulting Fragment has Void encoder.
    val values0: Tuple = Tuple.fromArray(values.toArray[Any])
    val voidEnc: Encoder[Void] = rowEnc.contramap[Void](_ => values0)
    val frag: Fragment[Void] = Fragment(List(Right(rowEnc.sql)), voidEnc, Origin.unknown)
    new InsertCommand[Cols, Void](table, projected, InsertSource.TypedRow(frag), conflict)
  }

  /** Batch — pre-applies each row to its own AppliedFragment. Args = Void. */
  private[sharp] def buildMany[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    rows: List[List[Any]],
    conflict: OnConflict
  ): InsertCommand[Cols, Void] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    val rowFrag: Fragment[Tuple] =
      Fragment(parts = List(Right(rowEnc.sql)), encoder = rowEnc, origin = Origin.unknown)
    val applied = rows.map(r => rowFrag(Tuple.fromArray(r.toArray[Any])))
    new InsertCommand[Cols, Void](table, projected, InsertSource.ManyRows(applied), conflict)
  }

  private[sharp] def buildFromQuery[Cols <: Tuple, Args](
    table: Table[Cols, ?],
    names: List[String],
    fragment: Fragment[Args],
    conflict: OnConflict
  ): InsertCommand[Cols, Args] =
    new InsertCommand[Cols, Args](table, lookupProjected(table, names), InsertSource.FromQuery(fragment), conflict)

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

/** INSERT entry point. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def insert: InsertBuilder[Cols] = new InsertBuilder[Cols](table)
}
