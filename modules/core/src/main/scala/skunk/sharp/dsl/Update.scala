package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, CompileChecks, RawConstants}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.sharp.where.Where.{FoldConcat, FoldConcatN}
import skunk.util.Origin

import scala.NamedTuple
import scala.deriving.Mirror
import scala.compiletime.constValueTuple

/**
 * UPDATE builder — compile-time staged so you can't accidentally run a rowset-nuking `UPDATE` with no WHERE.
 *
 * Threads two captured-args type parameters end-to-end:
 *
 *   - `SetArgs` — args from the SET RHS expressions.
 *   - `WArgs`   — args from the WHERE clause.
 *
 * `.compile` produces `CommandTemplate[Where.Concat[SetArgs, WArgs]]`.
 */
final class UpdateBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  /**
   * `SET <col := value>` — single-assignment form. `SetArgs` propagates from the assignment's `Args`,
   * so `c.email := Param[String]` produces `UpdateWithSet[..., String]` and the value is supplied at
   * execute time. `c.email := "literal"` produces `UpdateWithSet[..., Void]` (value-baked).
   */
  def set[A](f: ColumnsView[Cols] => SetAssignment[?, A]): UpdateWithSet[Cols, Name, A] = {
    val sa = f(table.columnsView)
    new UpdateWithSet[Cols, Name, A](table, sa.fragment)
  }

  /**
   * `SET (<col := v>, ...)` — tuple form for multiple assignments. `SetArgs` is fixed to `Void`: the
   * underlying typed Fragment encoder still composes whatever args the assignments contribute, but the
   * static `SetArgs` slot stays `Void`. For typed-Args across multiple SET items, use the `&` infix
   * combinator (`c.email := Param[String] & c.age := Param[Int]`) which threads `Concat[A1, A2]`.
   */
  @scala.annotation.targetName("setTuple")
  def set(f: ColumnsView[Cols] => Tuple): UpdateWithSet[Cols, Name, Void] = {
    val raw      = f(table.columnsView).toList.asInstanceOf[List[SetAssignment[?, ?]]]
    val combined = SetAssignment.combineAll(raw)
    new UpdateWithSet[Cols, Name, Void](table, combined)
  }

  inline def patch[R <: NamedTuple.AnyNamedTuple](p: R): UpdateWithSet[Cols, Name, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requirePatchValueTypes[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names  = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val values = p.asInstanceOf[Tuple].toList
    buildPatch(table, names, values)
  }

  inline def patch[T <: Product](p: T)(using m: Mirror.ProductOf[T]): UpdateWithSet[Cols, Name, Void] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requirePatchValueTypes[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names  = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val values = p.productIterator.toList
    buildPatch(table, names, values)
  }

  def from[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, Name *: EmptyTuple]
  ): UpdateFromBuilder[
    Cols,
    Name,
    SourceEntry[Table[Cols, Name], Cols, Cols, Name] *: SourceEntry[RR, CR, CR, AR] *: EmptyTuple
  ] = {
    val targetEntry =
      new SourceEntry[Table[Cols, Name], Cols, Cols, Name](
        table,
        table.currentAlias,
        table.columns,
        table.columns,
        JoinKind.Inner,
        None
      )
    val rel        = aR(other)
    val oCols      = rel.columns.asInstanceOf[CR]
    val otherEntry =
      new SourceEntry[RR, CR, CR, AR](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new UpdateFromBuilder[
      Cols,
      Name,
      SourceEntry[Table[Cols, Name], Cols, Cols, Name] *: SourceEntry[RR, CR, CR, AR] *: EmptyTuple
    ](table, targetEntry *: otherEntry *: EmptyTuple)
  }

}

private[sharp] def buildPatch[Cols <: Tuple, Name <: String & Singleton](
  table: Table[Cols, Name],
  names: List[String],
  values: List[Any]
): UpdateWithSet[Cols, Name, Void] = {
  val allCols = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
  val byName  = allCols.iterator.map(c => c.name.toString -> c).toMap
  val assignments: List[SetAssignment[?, ?]] =
    names.zip(values).collect { case (n, Some(v)) =>
      val col = byName(n)
      val tc  = TypedColumn.of(col.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      SetAssignment.fromValue[Any](tc, v.asInstanceOf[Any])(using
        new PgTypeFor[Any] { def codec: Codec[Any] = col.codec.asInstanceOf[Codec[Any]] }
      ).asInstanceOf[SetAssignment[?, ?]]
    }
  if (assignments.isEmpty)
    throw new IllegalArgumentException(
      "skunk-sharp: .patch(...) produced an empty SET list — every field was None. Postgres rejects UPDATE without SET; provide at least one Some(...)."
    )
  val combined = SetAssignment.combineAll(assignments)
  new UpdateWithSet[Cols, Name, Void](table, combined)
}

final class UpdateWithSet[Cols <: Tuple, Name <: String & Singleton, SetArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val setFragment: Fragment[?]
) {

  def where[A](f: ColumnsView[Cols] => Where[A]): UpdateReady[Cols, Name, SetArgs, A] = {
    val pred = f(table.columnsView)
    new UpdateReady[Cols, Name, SetArgs, A](table, setFragment, Some(pred.fragment))
  }

  def whereRaw(af: AppliedFragment): UpdateReady[Cols, Name, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af)
    new UpdateReady[Cols, Name, SetArgs, Any](table, setFragment, Some(combined))
  }

  def updateAll: UpdateReady[Cols, Name, SetArgs, Void] =
    new UpdateReady[Cols, Name, SetArgs, Void](table, setFragment, None)

}

final class UpdateReady[Cols <: Tuple, Name <: String & Singleton, SetArgs, WArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  def where[A](f: ColumnsView[Cols] => Where[A])(using
    c2: Where.Concat2[WArgs, A]
  ): UpdateReady[Cols, Name, SetArgs, Where.Concat[WArgs, A]] = {
    val pred     = f(table.columnsView)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred)
    new UpdateReady[Cols, Name, SetArgs, Where.Concat[WArgs, A]](table, setFragment, Some(combined))
  }

  def whereRaw(af: AppliedFragment)(using c2: Where.Concat2[WArgs, Void]): UpdateReady[Cols, Name, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af)
    new UpdateReady[Cols, Name, SetArgs, Any](table, setFragment, Some(combined))
  }

  private def updateParts: List[BodyPart] = {
    // setFragment is the typed SET-RHS chain; route to Right so its encoder threads SetArgs at execute time.
    // Value-baked assignments carry a Void-input encoder (contramapped to bake the value via Param.bind),
    // which the assemble walker handles uniformly with typed Param[T]-bearing assignments.
    val buf = scala.collection.mutable.ListBuffer[BodyPart](
      Left(table.updateSetHeader),
      Right(setFragment)
    )
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  def compile(using c2: Where.Concat2[SetArgs, WArgs]): CommandTemplate[Where.Concat[SetArgs, WArgs]] =
    MutationAssembly.command[SetArgs, WArgs](updateParts)

  def returning[T, A](f: ColumnsView[Cols] => TypedExpr[T, A])(using
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], A]
  ): QueryTemplate[Where.Concat[Where.Concat[SetArgs, WArgs], A], T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturningTyped[SetArgs, WArgs, A, T](updateParts, expr.fragment, expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](
    f: ColumnsView[Cols] => T
  )(using
    fc:   FoldConcatN[CollectArgs[T]],
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], FoldConcat[CollectArgs[T]]]
  ): QueryTemplate[Where.Concat[Where.Concat[SetArgs, WArgs], FoldConcat[CollectArgs[T]]], ExprOutputs[T]] = {
    val exprs    = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[T]]](exprs.map(_.fragment), ", ", fc.project)
    MutationAssembly.withReturningTyped[SetArgs, WArgs, FoldConcat[CollectArgs[T]], ExprOutputs[T]](
      updateParts, combined, codec
    )
  }

  def returningAll(using
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], Void]
  ): QueryTemplate[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    MutationAssembly.withReturningTyped[SetArgs, WArgs, Void, NamedRowOf[Cols]](updateParts, combined, codec)
      .asInstanceOf[QueryTemplate[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]]]
  }

}

// ---- UPDATE … FROM ---------------------------------------------------------------------------------

final class UpdateFromBuilder[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

  def from[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): UpdateFromBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]] = {
    val rel   = aR(other)
    val oCols = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new UpdateFromBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]](
      table,
      sources :* entry
    )
  }

  /** Single SET assignment — `SetArgs` propagates from the RHS expression's `Args`. */
  def set[A](f: JoinedView[Ss] => SetAssignment[?, A]): UpdateFromWithSet[Cols, Name, Ss, A] = {
    val sa = f(buildJoinedView(sources))
    new UpdateFromWithSet[Cols, Name, Ss, A](table, sources, sa.fragment)
  }

  /** Tuple-form SET — `SetArgs` widens to `Void`. Use `&` for typed-Args across multiple items. */
  @scala.annotation.targetName("setTuple")
  def set(f: JoinedView[Ss] => Tuple): UpdateFromWithSet[Cols, Name, Ss, Void] = {
    val raw      = f(buildJoinedView(sources)).toList.asInstanceOf[List[SetAssignment[?, ?]]]
    val combined = SetAssignment.combineAll(raw)
    new UpdateFromWithSet[Cols, Name, Ss, Void](table, sources, combined)
  }

}

final class UpdateFromWithSet[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, SetArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val setFragment: Fragment[?]
) {

  def where[A](f: JoinedView[Ss] => Where[A]): UpdateFromReady[Cols, Name, Ss, SetArgs, A] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, A](table, sources, setFragment, Some(pred.fragment))
  }

  def whereRaw(af: AppliedFragment): UpdateFromReady[Cols, Name, Ss, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Any](table, sources, setFragment, Some(combined))
  }

  def updateAll: UpdateFromReady[Cols, Name, Ss, SetArgs, Void] =
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Void](table, sources, setFragment, None)

}

final class UpdateFromReady[
  Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, SetArgs, WArgs
] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  def where[A](f: JoinedView[Ss] => Where[A])(using
    c2: Where.Concat2[WArgs, A]
  ): UpdateFromReady[Cols, Name, Ss, SetArgs, Where.Concat[WArgs, A]] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Where.Concat[WArgs, A]](table, sources, setFragment, Some(combined))
  }

  def whereRaw(af: AppliedFragment)(using c2: Where.Concat2[WArgs, Void]): UpdateFromReady[Cols, Name, Ss, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Any](table, sources, setFragment, Some(combined))
  }

  private def updateFromParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](
      Left(table.updateSetHeader),
      Right(setFragment)
    )
    val fromEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    if (fromEntries.nonEmpty) {
      buf += Left(TypedExpr.raw(" FROM "))
      buf += Left(TypedExpr.joined(fromEntries.map(aliasedFromEntry), ", "))
    }
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  def compile(using c2: Where.Concat2[SetArgs, WArgs]): CommandTemplate[Where.Concat[SetArgs, WArgs]] =
    MutationAssembly.command[SetArgs, WArgs](updateFromParts)

  def returning[T, A](f: JoinedView[Ss] => TypedExpr[T, A])(using
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], A]
  ): QueryTemplate[Where.Concat[Where.Concat[SetArgs, WArgs], A], T] = {
    val expr = f(buildJoinedView(sources))
    MutationAssembly.withReturningTyped[SetArgs, WArgs, A, T](updateFromParts, expr.fragment, expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](
    f: JoinedView[Ss] => T
  )(using
    fc:   FoldConcatN[CollectArgs[T]],
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], FoldConcat[CollectArgs[T]]]
  ): QueryTemplate[Where.Concat[Where.Concat[SetArgs, WArgs], FoldConcat[CollectArgs[T]]], ExprOutputs[T]] = {
    val exprs    = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[T]]](exprs.map(_.fragment), ", ", fc.project)
    MutationAssembly.withReturningTyped[SetArgs, WArgs, FoldConcat[CollectArgs[T]], ExprOutputs[T]](
      updateFromParts, combined, codec
    )
  }

  def returningAll(using
    c12:  Where.Concat2[SetArgs, WArgs],
    c123: Where.Concat2[Where.Concat[SetArgs, WArgs], Void]
  ): QueryTemplate[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    MutationAssembly.withReturningTyped[SetArgs, WArgs, Void, NamedRowOf[Cols]](updateFromParts, combined, codec)
      .asInstanceOf[QueryTemplate[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]]]
  }

}

// ---- SetAssignment ---------------------------------------------------------------------------------

/**
 * One typed `column = expression` assignment in an UPDATE SET list. `Args` is the captured-args type from the
 * RHS expression — `Void` when the RHS is a runtime value (baked via [[Param.bind]]), `T` when the RHS is a
 * deferred [[Param]], or whatever the wider expression's Args is.
 */
final class SetAssignment[T, Args] private[sharp] (
  private[sharp] val col: TypedColumn[T, ?, ?],
  private[sharp] val fragment: Fragment[Args]
)

object SetAssignment {

  /** `col := value` — value baked via [[Param.bind]]; Args = Void. */
  def fromValue[T](col: TypedColumn[T, ?, ?], value: T)(using pf: PgTypeFor[T]): SetAssignment[T, Void] = {
    val rhs = Param.bind(value)(using pf)
    fromExprAux[T, Void](col, rhs.fragment)
  }

  /** `col := expr` — RHS is any TypedExpr. Args from RHS propagates. */
  def fromExpr[T, A](col: TypedColumn[T, ?, ?], expr: TypedExpr[T, A]): SetAssignment[T, A] =
    fromExprAux[T, A](col, expr.fragment)

  private def fromExprAux[T, A](col: TypedColumn[T, ?, ?], rhsFrag: Fragment[A]): SetAssignment[T, A] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] =
      List[Either[String, cats.data.State[Int, String]]](Left(s""""${col.name}" = """)) ++ rhsFrag.parts
    val frag: Fragment[A] = Fragment(parts, rhsFrag.encoder, Origin.unknown)
    new SetAssignment[T, A](col, frag)
  }

  /** Combine a non-empty list of assignments into a single comma-joined `Fragment[?]`. */
  private[dsl] def combineAll(items: List[SetAssignment[?, ?]]): Fragment[?] = {
    require(items.nonEmpty, "skunk-sharp: cannot combine empty SET list")
    items.tail.foldLeft[Fragment[?]](items.head.fragment) { (acc, sa) =>
      val parts = acc.parts ++ RawConstants.COMMA_SEP.fragment.parts ++ sa.fragment.parts
      val enc   = SelectBuilder.combineEncoders(acc.encoder, sa.fragment.encoder)
      Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
    }
  }

  /** Infix `&` combinator — comma-style joining for tuple shapes. */
  extension [T1, A1](lhs: SetAssignment[T1, A1])
    def &[T2, A2](rhs: SetAssignment[T2, A2]): SetAssignment[Unit, Where.Concat[A1, A2]] = {
      val parts = lhs.fragment.parts ++ RawConstants.COMMA_SEP.fragment.parts ++ rhs.fragment.parts
      val enc   = SelectBuilder.combineEncoders(lhs.fragment.encoder, rhs.fragment.encoder)
      val frag: Fragment[Where.Concat[A1, A2]] = Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[Where.Concat[A1, A2]]]
      new SetAssignment[Unit, Where.Concat[A1, A2]](
        null.asInstanceOf[TypedColumn[Unit, ?, ?]], frag
      )
    }

}

extension [T, Null <: Boolean, N <: String & Singleton](col: TypedColumn[T, Null, N]) {

  /** `col = value` — value baked via [[Param.bind]]; contributes Void to the SetArgs slot. */
  def :=(value: T)(using pf: PgTypeFor[T]): SetAssignment[T, Void] =
    SetAssignment.fromValue(col, value)

  /** `col = <expression>` — RHS is any TypedExpr; its Args thread into the SetArgs slot. */
  def :=[A](expr: TypedExpr[T, A]): SetAssignment[T, A] =
    SetAssignment.fromExpr(col, expr)

}

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def update: UpdateBuilder[Cols, Name] = new UpdateBuilder[Cols, Name](table)
}
