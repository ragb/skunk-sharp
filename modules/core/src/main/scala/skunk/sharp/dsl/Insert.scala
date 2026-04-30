package skunk.sharp.dsl

import scala.annotation.targetName
import cats.Reducible
import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, CompileChecks, RawConstants}
import skunk.sharp.where.Where.{FoldConcat, FoldConcatN}
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
  inline def apply[R <: NamedTuple.AnyNamedTuple](row: R): InsertCommand[Cols, Void, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val vs    = row.asInstanceOf[Tuple].toList
    InsertCommand.buildSingleBaked[Cols](table, names, vs, AppliedFragment.empty)
  }

  /** Single-row insert from a case-class instance. Args = Void. */
  inline def apply[T <: Product](row: T)(using m: Mirror.ProductOf[T]): InsertCommand[Cols, Void, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val vs    = row.productIterator.toList
    InsertCommand.buildSingleBaked[Cols](table, names, vs, AppliedFragment.empty)
  }

  /** Batch from varargs. Values baked; Args = Void. */
  inline def values[R <: NamedTuple.AnyNamedTuple](row: R, more: R*): InsertCommand[Cols, Void, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rows  = (row :: more.toList).map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rows, AppliedFragment.empty)
  }

  /** Batch from `Reducible`. Args = Void. */
  inline def values[F[_]: Reducible, R <: NamedTuple.AnyNamedTuple](rows: F[R]): InsertCommand[Cols, Void, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.asInstanceOf[Tuple].toList)
    InsertCommand.buildMany[Cols](table, names, rs, AppliedFragment.empty)
  }

  inline def values[F[_]: Reducible, T <: Product](rows: F[T])(using m: Mirror.ProductOf[T]): InsertCommand[Cols, Void, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requireCoversRequired[Cols, m.MirroredElemLabels]
    CompileChecks.requireValueTypesMatch[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val rs    = Reducible[F].toNonEmptyList(rows).toList.map(_.productIterator.toList)
    InsertCommand.buildMany[Cols](table, names, rs, AppliedFragment.empty)
  }

  /** `INSERT INTO … SELECT …`. The inner subquery's `Args` threads via the `AsSubquery` evidence. */
  inline def from[Q, Row <: NamedTuple.AnyNamedTuple, Args](src: Q)(using
    ev: AsSubquery[Q, Row, Args]
  ): InsertCommand[Cols, Args, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[Row]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[Row], NamedTuple.DropNames[Row]]
    val names = constValueTuple[NamedTuple.Names[Row]].toList.asInstanceOf[List[String]]
    InsertCommand.buildFromQuery[Cols, Args](table, names, ev.fragment(src), AppliedFragment.empty)
  }

}

/**
 * The source of rows for an INSERT.
 */
sealed trait InsertSource

object InsertSource {

  /** A single baked-Void row fragment (values via [[Param.bind]]). Bound at Void at insertParts time. */
  final case class TypedRow(fragment: Fragment[?]) extends InsertSource

  /**
   * A single typed-Args row fragment — values are user-supplied at execute time via the typed `Args` slot.
   * Goes through the `Right` (typed) side of [[SelectBuilder.assemble]] so the encoder threads.
   */
  final case class TypedRowParams(fragment: Fragment[?]) extends InsertSource

  /** Many rows pre-applied. */
  final case class ManyRows(rows: List[AppliedFragment]) extends InsertSource

  /** `INSERT … SELECT` — the sub-query's typed fragment. */
  final case class FromQuery(fragment: Fragment[?]) extends InsertSource
}

/**
 * An assembled INSERT statement.
 *
 * `Args` is the captured-parameter type from the INSERT source (Void for value-baked inserts, or the row
 * tuple for `.withParams`). `CA` is the captured-parameter type from the `ON CONFLICT DO UPDATE SET`
 * clause (Void when there is no typed Param in the conflict clause).
 *
 * The conflict clause is stored as two parts:
 *  - `conflictHeaderAf` — the static SQL prefix (e.g. `" ON CONFLICT (id) DO UPDATE SET "` or
 *    `" ON CONFLICT DO NOTHING"` or the entire baked clause for the Tuple-form DO UPDATE).
 *  - `conflictSets` — the typed SET fragment (`CA`). `emptyVoidSlot` when the conflict clause is static.
 *
 * `insertParts` always emits exactly **two** `Right` slots in order:
 *   - Slot 0 (A1 = Args): the INSERT source. `emptyVoidSlot` for value-baked sources.
 *   - Slot 1 (A2 = CA): `conflictSets`. `emptyVoidSlot` when no typed conflict.
 *
 * The fixed-slot layout means `command[Args, CA]` and `withReturningTyped[Args, CA, ...]` dispatch
 * correctly for all combinations of baked/typed source and baked/typed conflict.
 */
final class InsertCommand[Cols <: Tuple, Args, CA] private[sharp] (
  private[sharp] val table: Table[Cols, ?],
  private[sharp] val projected: List[Column[?, ?, ?, ?]],
  private[sharp] val source: InsertSource,
  /** Static SQL for the conflict clause, pre-applied. `AppliedFragment.empty` when there is no conflict. */
  private[sharp] val conflictHeaderAf: AppliedFragment,
  /** Typed SET expressions for `ON CONFLICT DO UPDATE`. `emptyVoidSlot` when not a typed DO UPDATE. */
  private[sharp] val conflictSets: Fragment[CA]
) {

  def compile(using c2: Where.Concat2[Args, CA]): CommandTemplate[Where.Concat[Args, CA]] =
    MutationAssembly.command[Args, CA](insertParts)

  // ---- Body parts ---------------------------------------------------------------

  private def headerAf: AppliedFragment =
    if (projected.size == table.columns.size) table.insertIntoFullHeader
    else {
      val projections = projected.map(c => s""""${c.name}"""").mkString(", ")
      TypedExpr.raw(s"INSERT INTO ${table.qualifiedName} ($projections) ")
    }

  /**
   * Build the body-parts list. Always has two Right slots (source at 0, conflict sets at 1) so
   * `assemble[Args, CA, Void]` dispatches A1→source and CA→conflictSets correctly regardless of baking.
   */
  private[dsl] def insertParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(headerAf))
    source match {
      case InsertSource.TypedRow(f) =>
        buf += Left(RawConstants.VALUES)
        buf += Left(f.asInstanceOf[Fragment[Void]].apply(Void))
        buf += Right(SelectBuilder.emptyVoidSlot)  // A1 = Void placeholder (row already in Left)
      case InsertSource.TypedRowParams(f) =>
        buf += Left(RawConstants.VALUES)
        buf += Right(f)                            // A1 = Args
      case InsertSource.ManyRows(rows) =>
        buf += Left(TypedExpr.raw("VALUES "))
        buf += Left(TypedExpr.joined(rows, ", "))
        buf += Right(SelectBuilder.emptyVoidSlot)  // A1 = Void placeholder
      case InsertSource.FromQuery(frag) =>
        buf += Right(frag)                         // A1 = Args
    }
    if (conflictHeaderAf ne AppliedFragment.empty) buf += Left(conflictHeaderAf)
    buf += Right(conflictSets)                     // A2 = CA (or emptyVoid when no typed conflict)
    buf.toList
  }

  def returning[T, A](f: ColumnsView[Cols] => TypedExpr[T, A])(using
    c12:  Where.Concat2[Args, CA],
    c123: Where.Concat2[Where.Concat[Args, CA], A]
  ): QueryTemplate[Where.Concat[Where.Concat[Args, CA], A], T] = {
    val view = table.columnsView
    val expr = f(view)
    MutationAssembly.withReturningTyped[Args, CA, A, T](insertParts, expr.fragment, expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T)(using
    fc:   FoldConcatN[CollectArgs[T]],
    c12:  Where.Concat2[Args, CA],
    c123: Where.Concat2[Where.Concat[Args, CA], FoldConcat[CollectArgs[T]]]
  ): QueryTemplate[Where.Concat[Where.Concat[Args, CA], FoldConcat[CollectArgs[T]]], ExprOutputs[T]] = {
    val view     = table.columnsView
    val exprs    = f(view).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[T]]](exprs.map(_.fragment), ", ", fc.project)
    MutationAssembly.withReturningTyped[Args, CA, FoldConcat[CollectArgs[T]], ExprOutputs[T]](
      insertParts, combined, codec
    )
  }

  def returningAll(using
    c12:  Where.Concat2[Args, CA],
    c123: Where.Concat2[Where.Concat[Args, CA], Void]
  ): QueryTemplate[Where.Concat[Args, CA], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    MutationAssembly.withReturningTyped[Args, CA, Void, NamedRowOf[Cols]](insertParts, combined, codec)
      .asInstanceOf[QueryTemplate[Where.Concat[Args, CA], NamedRowOf[Cols]]]
  }

  // ---- ON CONFLICT ----

  def onConflictDoNothing: InsertCommand[Cols, Args, Void] =
    InsertCommand.mk(table, projected, source, TypedExpr.raw(" ON CONFLICT DO NOTHING"), SelectBuilder.emptyVoidSlot)

  def onConflict[T, Null <: Boolean, N <: String & Singleton](
    f: ColumnsView[Cols] => TypedColumn[T, Null, N]
  )(using
    ev: HasUniqueness[Cols, N] =:= true
  ): OnConflictBuilder[Cols, Args] = {
    val col = f(table.columnsView)
    OnConflictBuilder(
      InsertCommand.mk(table, projected, source, AppliedFragment.empty, SelectBuilder.emptyVoidSlot),
      List(col.name)
    )
  }

  def onConflictComposite[T <: NonEmptyTuple](
    f: ColumnsView[Cols] => T
  )(using
    ev: HasCompositeUniqueness[Cols, NamesOfTypedCols[T]] =:= true
  ): OnConflictBuilder[Cols, Args] = {
    val names = f(table.columnsView).toList.asInstanceOf[List[TypedColumn[?, ?, ?]]].map(_.name)
    OnConflictBuilder(
      InsertCommand.mk(table, projected, source, AppliedFragment.empty, SelectBuilder.emptyVoidSlot),
      names
    )
  }

  private[sharp] def tableColumns: Cols = table.columns
}

object InsertCommand {

  private[dsl] def mk[Cols <: Tuple, Args, CA](
    table: Table[Cols, ?],
    projected: List[Column[?, ?, ?, ?]],
    source: InsertSource,
    conflictHeaderAf: AppliedFragment,
    conflictSets: Fragment[CA]
  ): InsertCommand[Cols, Args, CA] =
    new InsertCommand[Cols, Args, CA](table, projected, source, conflictHeaderAf, conflictSets)

  /**
   * Single-row insert from `Param[T]` placeholders — Args = the row tuple. The row encoder is built from
   * each Param's codec; user supplies the tuple at execute time.
   */
  private[sharp] def buildSingleParams[Cols <: Tuple, Args](
    table: Table[Cols, ?],
    names: List[String],
    params: List[Param[?]],
    conflictHeaderAf: AppliedFragment
  ): InsertCommand[Cols, Args, Void] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(params.map(_.codec))
    val rowEnc               = perRow.values
    val frag: Fragment[Args] = Fragment(List(Right(rowEnc.sql)), rowEnc.asInstanceOf[Encoder[Args]], Origin.unknown)
    mk(table, projected, InsertSource.TypedRowParams(frag), conflictHeaderAf, SelectBuilder.emptyVoidSlot)
  }

  /** Build a single-row insert with values baked via Param.bind. Args = Void. */
  private[sharp] def buildSingleBaked[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    values: List[Any],
    conflictHeaderAf: AppliedFragment
  ): InsertCommand[Cols, Void, Void] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    val values0: Tuple = Tuple.fromArray(values.toArray[Any])
    val voidEnc: Encoder[Void] = rowEnc.contramap[Void](_ => values0)
    val frag: Fragment[Void] = Fragment(List(Right(rowEnc.sql)), voidEnc, Origin.unknown)
    mk(table, projected, InsertSource.TypedRow(frag), conflictHeaderAf, SelectBuilder.emptyVoidSlot)
  }

  /** Batch — pre-applies each row to its own AppliedFragment. Args = Void. */
  private[sharp] def buildMany[Cols <: Tuple](
    table: Table[Cols, ?],
    names: List[String],
    rows: List[List[Any]],
    conflictHeaderAf: AppliedFragment
  ): InsertCommand[Cols, Void, Void] = {
    val projected = lookupProjected(table, names)
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc               = perRow.values
    val rowFrag: Fragment[Tuple] =
      Fragment(parts = List(Right(rowEnc.sql)), encoder = rowEnc, origin = Origin.unknown)
    val applied = rows.map(r => rowFrag(Tuple.fromArray(r.toArray[Any])))
    mk(table, projected, InsertSource.ManyRows(applied), conflictHeaderAf, SelectBuilder.emptyVoidSlot)
  }

  private[sharp] def buildFromQuery[Cols <: Tuple, Args](
    table: Table[Cols, ?],
    names: List[String],
    fragment: Fragment[Args],
    conflictHeaderAf: AppliedFragment
  ): InsertCommand[Cols, Args, Void] =
    mk(table, lookupProjected(table, names), InsertSource.FromQuery(fragment), conflictHeaderAf, SelectBuilder.emptyVoidSlot)

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

/**
 * Continuation after `.onConflict(col)`. Holds a Void-CA base command so that `doUpdate[CA]` can return
 * a fresh `InsertCommand[Cols, Args, CA]` with the correct CA type.
 */
final class OnConflictBuilder[Cols <: Tuple, Args] private[sharp] (
  private val cmd: InsertCommand[Cols, Args, Void],
  private val cols: List[String]
) {

  private def quotedCols: String = cols.map(c => s""""$c"""").mkString(", ")
  private def doNothingHeader: AppliedFragment =
    TypedExpr.raw(s" ON CONFLICT ($quotedCols) DO NOTHING")
  private def doUpdateHeader: AppliedFragment =
    TypedExpr.raw(s" ON CONFLICT ($quotedCols) DO UPDATE SET ")

  def doNothing: InsertCommand[Cols, Args, Void] =
    InsertCommand.mk(cmd.table, cmd.projected, cmd.source, doNothingHeader, SelectBuilder.emptyVoidSlot)

  /**
   * Typed SET — the lambda returns a single `SetAssignment[?, CA]` (possibly `&`-chained). `CA` propagates
   * to `InsertCommand` and surfaces in `.compile`'s `CommandTemplate[Concat[Args, CA]]`. Use [[Param]] in
   * the RHS to defer values to execute time; baked RHS values (`:= "x"`) yield `CA = Void`.
   *
   * For multiple baked-value assignments, the Tuple overload (`.doUpdate(c => (c.a := "x", c.b := 1))`)
   * is safer: it pre-applies all values into `Left(AF)` to avoid a product-encoder issue that would arise
   * when two baked encoders are combined via `&`.
   */
  def doUpdate[CA](f: ColumnsView[Cols] => SetAssignment[?, CA]): InsertCommand[Cols, Args, CA] = {
    val sa = f(ColumnsView(cmd.tableColumns))
    InsertCommand.mk(
      cmd.table, cmd.projected, cmd.source,
      doUpdateHeader,
      sa.fragment.asInstanceOf[Fragment[CA]]
    )
  }

  /**
   * Tuple SET — multiple baked-value assignments. All SET RHS values are pre-applied into `Left(AF)`,
   * so `CA = Void` and the conflict contributes no runtime parameters. The baked clause (header +
   * pre-applied SET values) is stored as a single `Left(AppliedFragment)`.
   */
  @targetName("doUpdateTuple")
  def doUpdate(f: ColumnsView[Cols] => Tuple): InsertCommand[Cols, Args, Void] = {
    val view   = ColumnsView(cmd.tableColumns)
    val raw    = f(view).toList.asInstanceOf[List[SetAssignment[?, ?]]]
    val setsAF = TypedExpr.joined(raw.map(sa => sa.fragment.asInstanceOf[Fragment[Void]].apply(Void)), ", ")
    InsertCommand.mk(
      cmd.table, cmd.projected, cmd.source,
      doUpdateHeader |+| setsAF,
      SelectBuilder.emptyVoidSlot
    )
  }

  /**
   * Typed SET with access to the `excluded` pseudo-table. `CA` propagates from the assignment's Args.
   */
  def doUpdateFromExcluded[CA](
    f: (ColumnsView[Cols], ColumnsView[Cols]) => SetAssignment[?, CA]
  ): InsertCommand[Cols, Args, CA] = {
    val target   = ColumnsView(cmd.tableColumns)
    val excluded = ColumnsView.qualifiedRaw(cmd.tableColumns, "excluded")
    val sa       = f(target, excluded)
    InsertCommand.mk(
      cmd.table, cmd.projected, cmd.source,
      doUpdateHeader,
      sa.fragment.asInstanceOf[Fragment[CA]]
    )
  }

  /**
   * Tuple SET with `excluded` pseudo-table. All values pre-applied; `CA = Void`.
   */
  @targetName("doUpdateFromExcludedTuple")
  def doUpdateFromExcluded(
    f: (ColumnsView[Cols], ColumnsView[Cols]) => Tuple
  ): InsertCommand[Cols, Args, Void] = {
    val target   = ColumnsView(cmd.tableColumns)
    val excluded = ColumnsView.qualifiedRaw(cmd.tableColumns, "excluded")
    val raw      = f(target, excluded).toList.asInstanceOf[List[SetAssignment[?, ?]]]
    val setsAF   = TypedExpr.joined(raw.map(sa => sa.fragment.asInstanceOf[Fragment[Void]].apply(Void)), ", ")
    InsertCommand.mk(
      cmd.table, cmd.projected, cmd.source,
      doUpdateHeader |+| setsAF,
      SelectBuilder.emptyVoidSlot
    )
  }

}

/** INSERT entry point. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def insert: InsertBuilder[Cols] = new InsertBuilder[Cols](table)
}

/** Strip the `Param[_]` wrapper from each tuple element: `(Param[A], Param[B]) → (A, B)`. */
type StripParams[T <: Tuple] <: Tuple = T match {
  case EmptyTuple        => EmptyTuple
  case Param[t] *: tail  => t *: StripParams[tail]
}

extension [Cols <: Tuple](b: InsertBuilder[Cols]) {

  /**
   * Typed-Args INSERT: every named-tuple field is a `Param[T]`, and the resulting `CommandTemplate`'s
   * `Args` is the row tuple (`StripParams[…]`). Values are supplied at execute via `cmd.run(s)(args)`.
   *
   * {{{
   *   val create: CommandTemplate[(UUID, String, Int)] =
   *     users.insert.withParams((id = Param[UUID], email = Param[String], age = Param[Int])).compile
   *   prep <- create.prepared(s)
   *   _    <- prep.execute((uid, "x@y", 30))
   * }}}
   *
   * Compile checks: every field name exists on `Cols` and every required (non-defaulted) column is covered.
   * The runtime walk extracts each `Param[T]`'s codec to build the row encoder.
   */
  inline def withParams[R <: NamedTuple.AnyNamedTuple](
    row: R
  ): InsertCommand[Cols, StripParams[NamedTuple.DropNames[R]], Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    val names  = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val params = row.asInstanceOf[Tuple].toList.map {
      case p: Param[?] => p
      case other       =>
        throw new IllegalArgumentException(
          s"skunk-sharp: .withParams expects every field to be a Param[T]; got: $other (${other.getClass.getName})"
        )
    }
    InsertCommand.buildSingleParams[Cols, StripParams[NamedTuple.DropNames[R]]](b.table, names, params, AppliedFragment.empty)
  }

}
