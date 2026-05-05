package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, RawConstants}
import skunk.sharp.where.Where
import skunk.sharp.where.Where.{FoldConcat, FoldConcatN}

/**
 * DELETE builder — compile-time staged so you can't accidentally run a `DELETE FROM …` with no WHERE.
 *
 * State machine:
 *
 *   1. `users.delete` → [[DeleteBuilder]] (entry, `Args = Void`).
 *   2. `.where(_ => Where[A])` → [[DeleteReady]] with `Args = A`. Subsequent `.where` extends `Args` to
 *      `Where.Concat[Args, A2]`.
 *   3. `.deleteAll` → [[DeleteReady]] with `Args = Void`.
 *   4. `.compile`, `.returning*` available on [[DeleteReady]].
 */
final class DeleteBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  def where[A](f: ColumnsView[Cols] => Where[A]): DeleteReady[Cols, Name, A] = {
    val pred = f(table.columnsView)
    new DeleteReady[Cols, Name, A](table, Some(pred.fragment))
  }

  def whereRaw(af: AppliedFragment): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af)
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  def deleteAll: DeleteReady[Cols, Name, Void] =
    new DeleteReady[Cols, Name, Void](table, None)

  def using[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, Name *: EmptyTuple]
  ): DeleteUsingBuilder[
    Cols,
    Name,
    SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void] *: SourceEntry[RR, CR, CR, AR, Void] *: EmptyTuple
  ] = {
    val targetEntry =
      new SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void](
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
      new SourceEntry[RR, CR, CR, AR, Void](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new DeleteUsingBuilder[
      Cols,
      Name,
      SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void] *: SourceEntry[RR, CR, CR, AR, Void] *: EmptyTuple
    ](table, targetEntry *: otherEntry *: EmptyTuple)
  }

}

final class DeleteReady[Cols <: Tuple, Name <: String & Singleton, Args] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  def where[A](f: ColumnsView[Cols] => Where[A])(using
    c2: Where.Concat2[Args, A]
  ): DeleteReady[Cols, Name, Where.Concat[Args, A]] = {
    val pred     = f(table.columnsView)
    val combined = SelectBuilder.andInto[Args, A](whereOpt.asInstanceOf[Option[Fragment[Args]]], pred)
    new DeleteReady[Cols, Name, Where.Concat[Args, A]](table, Some(combined))
  }

  def whereRaw(af: AppliedFragment)(using c2: Where.Concat2[Args, Void]): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto[Args](whereOpt.asInstanceOf[Option[Fragment[Args]]], af)
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  private def deleteParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(table.deleteFromHeader))
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  def compile: CommandTemplate[Args] = MutationAssembly.command[Args, Void](deleteParts).asInstanceOf[CommandTemplate[Args]]

  def returning[T, A](f: ColumnsView[Cols] => TypedExpr[T, A])(using
    c2: Where.Concat2[Args, A]
  ): QueryTemplate[Where.Concat[Args, A], T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturningTyped2[Args, A, T](deleteParts, expr.fragment, expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T)(using
    fc: FoldConcatN[CollectArgs[T]],
    c2: Where.Concat2[Args, FoldConcat[CollectArgs[T]]]
  ): QueryTemplate[Where.Concat[Args, FoldConcat[CollectArgs[T]]], ExprOutputs[T]] = {
    val exprs    = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[T]]](exprs.map(_.fragment), ", ", fc.project)
    MutationAssembly.withReturningTyped2[Args, FoldConcat[CollectArgs[T]], ExprOutputs[T]](
      deleteParts, combined, codec
    )
  }

  /**
   * `RETURNING <named tuple of TypedExprs>` — projects multiple columns into a labelled tuple
   * (`(id = u.id, email = u.email)`). Args of each item thread into the outer query via
   * [[Where.FoldConcatN]] (drops `Void` slots so plain column refs collapse out).
   */
  def returningNamed[NT <: scala.NamedTuple.AnyNamedTuple](f: ColumnsView[Cols] => NT)(using
    fc: FoldConcatN[CollectArgs[scala.NamedTuple.DropNames[NT]]],
    c2: Where.Concat2[Args, FoldConcat[CollectArgs[scala.NamedTuple.DropNames[NT]]]]
  ): QueryTemplate[
    Where.Concat[Args, FoldConcat[CollectArgs[scala.NamedTuple.DropNames[NT]]]],
    scala.NamedTuple.NamedTuple[scala.NamedTuple.Names[NT], ExprOutputs[scala.NamedTuple.DropNames[NT]]]
  ] = {
    type Vs = scala.NamedTuple.DropNames[NT]
    type Ns = scala.NamedTuple.Names[NT]
    type R  = scala.NamedTuple.NamedTuple[Ns, ExprOutputs[Vs]]
    val tup      = f(table.columnsView).asInstanceOf[Product]
    val exprs    = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[R]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[Vs]]](exprs.map(_.fragment), ", ", fc.project)
    MutationAssembly.withReturningTyped2[Args, FoldConcat[CollectArgs[Vs]], R](
      deleteParts, combined, codec
    )
  }

  def returningAll(using
    c2: Where.Concat2[Args, Void]
  ): QueryTemplate[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    MutationAssembly.withReturningTyped2[Args, Void, NamedRowOf[Cols]](deleteParts, combined, codec)
      .asInstanceOf[QueryTemplate[Args, NamedRowOf[Cols]]]
  }

}

/**
 * Shared command/RETURNING assembly for mutation builders (DELETE / UPDATE / INSERT). Routes through
 * [[SelectBuilder.assemble]] for the typed Fragment[Args] composition, then wraps as either a
 * [[CommandTemplate]] or [[QueryTemplate]].
 */
private[dsl] object MutationAssembly {

  /**
   * Mutation builders carry at most two typed slots in render order — for UPDATE that's `SET` then `WHERE`;
   * INSERT and DELETE collapse to one (or zero). Callers pass the two slot args as `[A1, A2]` and
   * `command` returns a `CommandTemplate[Concat[A1, A2]]`. When a slot is unused, pass `Void` for it; the
   * `Concat2` priority chain keeps the typeclass resolvable.
   */
  def command[A1, A2](parts: List[BodyPart])(using c2: Where.Concat2[A1, A2]): CommandTemplate[Where.Concat[A1, A2]] = {
    val tpl = SelectBuilder.assemble[A1, A2, Void](parts, Nil, Void.codec)(using c2)
    CommandTemplate.mk[Where.Concat[A1, A2]](tpl.fragment.asInstanceOf[Fragment[Where.Concat[A1, A2]]])
  }

  /**
   * Three-slot RETURNING that threads typed Args — used by UPDATE (SET + WHERE + RETURNING). The single
   * Fragment's `RetArgs` becomes the third typed slot at execute time, after SET/WHERE. Multi-item
   * callers combine their items via [[TypedExpr.combineList]] before calling.
   */
  def withReturningTyped[A1, A2, RetArgs, R](
    base: List[BodyPart],
    returning: Fragment[RetArgs],
    codec: Codec[R]
  )(using
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], RetArgs]
  ): QueryTemplate[Where.Concat[Where.Concat[A1, A2], RetArgs], R] = {
    val parts: List[BodyPart] = base ++ List[BodyPart](Left(RawConstants.RETURNING), Right(returning))
    SelectBuilder.assemble3[A1, A2, RetArgs, R](parts, Nil, codec)(using c12, c123)
  }

  /**
   * Two-slot RETURNING — used by DELETE (WHERE + RETURNING) and INSERT (VALUES + RETURNING). The
   * walker maps Right slot 0 → A1 and Right slot 1 → A2 in render order.
   */
  def withReturningTyped2[A1, RetArgs, R](
    base: List[BodyPart],
    returning: Fragment[RetArgs],
    codec: Codec[R]
  )(using c2: Where.Concat2[A1, RetArgs]): QueryTemplate[Where.Concat[A1, RetArgs], R] = {
    val parts: List[BodyPart] = base ++ List[BodyPart](Left(RawConstants.RETURNING), Right(returning))
    SelectBuilder.assemble[A1, RetArgs, R](parts, Nil, codec)(using c2)
  }

}

// ---- DELETE … USING -------------------------------------------------------------------------------

final class DeleteUsingBuilder[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

  def using[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): DeleteUsingBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]]] = {
    val rel   = aR(other)
    val oCols = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR, Void](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new DeleteUsingBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]]](
      table,
      sources :* entry
    )
  }

  def where[A](f: JoinedView[Ss] => Where[A]): DeleteUsingReady[Cols, Name, Ss, A] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    new DeleteUsingReady[Cols, Name, Ss, A](table, sources, Some(pred.fragment))
  }

  def whereRaw(af: AppliedFragment): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af)
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  def deleteAll: DeleteUsingReady[Cols, Name, Ss, Void] =
    new DeleteUsingReady[Cols, Name, Ss, Void](table, sources, None)

}

final class DeleteUsingReady[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, Args] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  def where[A](f: JoinedView[Ss] => Where[A])(using
    c2: Where.Concat2[Args, A]
  ): DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]] = {
    val view     = buildJoinedView(sources)
    val pred     = f(view)
    val combined = SelectBuilder.andInto[Args, A](whereOpt.asInstanceOf[Option[Fragment[Args]]], pred)
    new DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]](table, sources, Some(combined))
  }

  def whereRaw(af: AppliedFragment)(using c2: Where.Concat2[Args, Void]): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto[Args](whereOpt.asInstanceOf[Option[Fragment[Args]]], af)
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  /**
   * DELETE … USING <tail sources> WHERE — emits per-source body Right slots so typed-subquery USING sources
   * thread their inner Args into the outer command's args.
   */
  private def bodyParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(table.deleteFromHeader))
    val usingEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].tail
    if (usingEntries.nonEmpty) {
      buf += Left(RawConstants.USING)
      var first = true
      usingEntries.foreach { s =>
        if (first) first = false else buf += Left(TypedExpr.raw(", "))
        aliasedFromEntryParts(s).foreach(buf += _)
      }
    }
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  private def usingTailBodyArgs(bff: SourceBodyArgsProj[? <: Tuple], sArgs: Any): List[Any] =
    bff.project(sArgs) match {
      case _ :: rest => rest // drop head (target table — always Void)
      case _         => Nil
    }

  // Concat-chain: SArgs ⊕ Args (WHERE).
  def compile[SArgs](using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:  SourceBodyArgsProj[Ss],
    sw:   Where.Concat2[SArgs, Args]
  ): CommandTemplate[Where.Concat[SArgs, Args]] = {
    type Out = Where.Concat[SArgs, Args]
    val slotValues: Out => IArray[Any] = args => {
      val (sArgs, wArgs) = sw.project(args)
      val perTailBody    = usingTailBodyArgs(bff, sArgs)
      val out = scala.collection.mutable.ArrayBuffer.empty[Any]
      perTailBody.foreach(out += _)
      out += wArgs
      IArray.from(out)
    }
    val tpl = SelectBuilder.assembleN[Out, Void](bodyParts, Nil, Void.codec, slotValues)
    CommandTemplate.mk[Out](tpl.fragment)
  }

  // Concat-chain: SArgs ⊕ Args (WHERE) ⊕ A (RETURNING).
  def returning[T, A, SArgs](f: JoinedView[Ss] => TypedExpr[T, A])(using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:  SourceBodyArgsProj[Ss],
    sw:   Where.Concat2[SArgs, Args],
    swR:  Where.Concat2[Where.Concat[SArgs, Args], A]
  ): QueryTemplate[Where.Concat[Where.Concat[SArgs, Args], A], T] = {
    val expr = f(buildJoinedView(sources))
    val parts: List[BodyPart] = bodyParts ++ List[BodyPart](Left(RawConstants.RETURNING), Right(expr.fragment))
    type Out = Where.Concat[Where.Concat[SArgs, Args], A]
    val slotValues: Out => IArray[Any] = args => {
      val (swAcc, retArgs) = swR.project(args)
      val (sArgs, wArgs)   = sw.project(swAcc.asInstanceOf[Where.Concat[SArgs, Args]])
      val perTailBody      = usingTailBodyArgs(bff, sArgs)
      val out = scala.collection.mutable.ArrayBuffer.empty[Any]
      perTailBody.foreach(out += _)
      out += wArgs
      out += retArgs
      IArray.from(out)
    }
    SelectBuilder.assembleN[Out, T](parts, Nil, expr.codec, slotValues)
  }

  def returningTuple[T <: NonEmptyTuple, SArgs](f: JoinedView[Ss] => T)(using
    fc:   FoldConcatN[CollectArgs[T]],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:  SourceBodyArgsProj[Ss],
    sw:   Where.Concat2[SArgs, Args],
    swR:  Where.Concat2[Where.Concat[SArgs, Args], FoldConcat[CollectArgs[T]]]
  ): QueryTemplate[Where.Concat[Where.Concat[SArgs, Args], FoldConcat[CollectArgs[T]]], ExprOutputs[T]] = {
    val exprs    = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[T]]](exprs.map(_.fragment), ", ", fc.project)
    returning[ExprOutputs[T], FoldConcat[CollectArgs[T]], SArgs](_ =>
      TypedExpr[ExprOutputs[T], FoldConcat[CollectArgs[T]]](combined, codec)
    )(using sbOf, bff, sw, swR)
  }

  def returningNamed[NT <: scala.NamedTuple.AnyNamedTuple, SArgs](f: JoinedView[Ss] => NT)(using
    fc:   FoldConcatN[CollectArgs[scala.NamedTuple.DropNames[NT]]],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:  SourceBodyArgsProj[Ss],
    sw:   Where.Concat2[SArgs, Args],
    swR:  Where.Concat2[Where.Concat[SArgs, Args], FoldConcat[CollectArgs[scala.NamedTuple.DropNames[NT]]]]
  ): QueryTemplate[
    Where.Concat[Where.Concat[SArgs, Args], FoldConcat[CollectArgs[scala.NamedTuple.DropNames[NT]]]],
    scala.NamedTuple.NamedTuple[scala.NamedTuple.Names[NT], ExprOutputs[scala.NamedTuple.DropNames[NT]]]
  ] = {
    type Vs = scala.NamedTuple.DropNames[NT]
    type Ns = scala.NamedTuple.Names[NT]
    type R  = scala.NamedTuple.NamedTuple[Ns, ExprOutputs[Vs]]
    val tup      = f(buildJoinedView(sources)).asInstanceOf[Product]
    val exprs    = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[R]]
    val combined = TypedExpr.combineList[FoldConcat[CollectArgs[Vs]]](exprs.map(_.fragment), ", ", fc.project)
    returning[R, FoldConcat[CollectArgs[Vs]], SArgs](_ =>
      TypedExpr[R, FoldConcat[CollectArgs[Vs]]](combined, codec)
    )(using sbOf, bff, sw, swR)
  }

  def returningAll[SArgs](using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:  SourceBodyArgsProj[Ss],
    sw:   Where.Concat2[SArgs, Args],
    swR:  Where.Concat2[Where.Concat[SArgs, Args], Void]
  ): QueryTemplate[Where.Concat[SArgs, Args], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    returning[NamedRowOf[Cols], Void, SArgs](_ =>
      TypedExpr[NamedRowOf[Cols], Void](combined, codec)
    )(using sbOf, bff, sw, swR)
      .asInstanceOf[QueryTemplate[Where.Concat[SArgs, Args], NamedRowOf[Cols]]]
  }

}

// ---- BodyPart re-export ---------------------------------------------------------------------------

private[dsl] type BodyPart = SelectBuilder.BodyPart

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols, Name] = new DeleteBuilder[Cols, Name](table)
}
