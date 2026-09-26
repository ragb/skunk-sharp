package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{RawConstants, RowCodecs}, RowCodecs.tupleCodec
import skunk.sharp.where.Where

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

  inline def where[A](f: ColumnsView[Cols] => Where[A]): DeleteReady[Cols, Name, A] = {
    val pred = f(table.columnsView)
    new DeleteReady[Cols, Name, A](table, Some(pred.fragment))
  }

  inline def whereRaw(af: AppliedFragment): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af, Where.projPair[Void, Void])
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  inline def deleteAll: DeleteReady[Cols, Name, Void] =
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

final class DeleteReady[
  Cols <: Tuple,
  Name <: String & Singleton,
  Args
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  inline def where[A](f: ColumnsView[Cols] => Where[A]): DeleteReady[Cols, Name, Where.Concat[Args, A]] = {
    val pred     = f(table.columnsView)
    val combined = SelectBuilder.andInto[Args, A](
      whereOpt.asInstanceOf[Option[Fragment[Args]]],
      pred,
      Where.projPair[Args, A]
    )
    new DeleteReady[Cols, Name, Where.Concat[Args, A]](table, Some(combined))
  }

  inline def whereRaw(af: AppliedFragment): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto[Args](
      whereOpt.asInstanceOf[Option[Fragment[Args]]],
      af,
      Where.projPair[Args, Void]
    )
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  private def deleteParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](SelectBuilder.bake(table.deleteFromHeader))
    whereOpt.foreach { f =>
      buf += SelectBuilder.bake(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  inline def compile: CommandTemplate[Args] =
    MutationAssembly.command[Args, Void](deleteParts).asInstanceOf[CommandTemplate[Args]]

  inline def returning[T, A](f: ColumnsView[Cols] => TypedExpr[T, A]): QueryTemplate[Where.Concat[Args, A], T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturningTyped2[Args, A, T](deleteParts, expr.fragment, expr.codec)
  }

  inline def returningTuple[T <: NonEmptyTuple, TOut](f: ColumnsView[Cols] => T)(using
    pa: ProjArgsOf.Aux[T, TOut]
  ): QueryTemplate[Where.Concat[Args, TOut], ExprOutputs[T]] = {
    val exprs    = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    MutationAssembly.withReturningTyped2[Args, TOut, ExprOutputs[T]](deleteParts, combined, codec)
  }

  inline def returningNamed[NT <: scala.NamedTuple.AnyNamedTuple, TOut](f: ColumnsView[Cols] => NT)(using
    pa: ProjArgsOf.Aux[scala.NamedTuple.DropNames[NT], TOut]
  ): QueryTemplate[
    Where.Concat[Args, TOut],
    scala.NamedTuple.NamedTuple[scala.NamedTuple.Names[NT], ExprOutputs[scala.NamedTuple.DropNames[NT]]]
  ] = {
    type Vs = scala.NamedTuple.DropNames[NT]
    type Ns = scala.NamedTuple.Names[NT]
    type R  = scala.NamedTuple.NamedTuple[Ns, ExprOutputs[Vs]]
    val tup      = f(table.columnsView).asInstanceOf[Product]
    val exprs    = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[R]]
    val combined = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    MutationAssembly.withReturningTyped2[Args, TOut, R](deleteParts, combined, codec)
  }

  inline def returningAll: QueryTemplate[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.RowCodecs.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    MutationAssembly.withReturningTyped2[Args, Void, NamedRowOf[Cols]](deleteParts, combined, codec)
      .asInstanceOf[QueryTemplate[Args, NamedRowOf[Cols]]]
  }

}

/**
 * Shared command/RETURNING assembly for mutation builders (DELETE / UPDATE / INSERT). Routes through
 * [[SelectBuilder.assembleN]] for the typed Fragment[Args] composition, then wraps as either a [[CommandTemplate]] or
 * [[QueryTemplate]].
 */
private[dsl] object MutationAssembly {

  // The inline entry points only compute the slots' shape codes (the one thing that needs the concrete Args types);
  // the split and assembly are shared non-inline bodies, not a copy per call site.

  inline def command[A1, A2](parts: List[BodyPart]): CommandTemplate[Where.Concat[A1, A2]] =
    commandImpl[Where.Concat[A1, A2]](parts, Where.slotCodes[(A1, A2)])

  def commandImpl[Out](parts: List[BodyPart], codes: Tuple): CommandTemplate[Out] = {
    val split = new skunk.sharp.where.SlotSplit(codes)
    val tpl   = SelectBuilder.assembleN[Out, Void](parts, Nil, Void.codec, split)
    CommandTemplate.mk[Out](tpl.fragment)
  }

  /** Three-slot RETURNING that threads typed Args — used by UPDATE (SET + WHERE + RETURNING) and INSERT. */
  inline def withReturningTyped[A1, A2, RetArgs, R](
    base: List[BodyPart],
    ret: Fragment[RetArgs],
    codec: Codec[R]
  ): QueryTemplate[Where.Concat[Where.Concat[A1, A2], RetArgs], R] =
    returningImpl[Where.Concat[Where.Concat[A1, A2], RetArgs], R](base, ret, codec, Where.slotCodes[(A1, A2, RetArgs)])

  /** Two-slot RETURNING — used by DELETE (WHERE + RETURNING). */
  inline def withReturningTyped2[A1, RetArgs, R](
    base: List[BodyPart],
    ret: Fragment[RetArgs],
    codec: Codec[R]
  ): QueryTemplate[Where.Concat[A1, RetArgs], R] =
    returningImpl[Where.Concat[A1, RetArgs], R](base, ret, codec, Where.slotCodes[(A1, RetArgs)])

  /**
   * Assembly for statements with extra sources (UPDATE … FROM, DELETE … USING, MERGE): slot `srcIdx` holds the sources'
   * combined body Args, expanded per source via `bff` (dropping the target at the head, always Void).
   */
  def withSourcesImpl[Out, R](
    parts: List[BodyPart],
    codec: Codec[R],
    codes: Tuple,
    bff: SourceBodyArgsProj[? <: Tuple],
    srcIdx: Int
  ): QueryTemplate[Out, R] = {
    val sp = new skunk.sharp.where.SlotSplit(codes)
    SelectBuilder.assembleN[Out, R](
      parts,
      Nil,
      codec,
      args => {
        val v    = sp(args)
        val tail = bff.project(v(srcIdx)) match {
          case _ :: rest => rest
          case _         => Nil
        }
        IArray.from(v.take(srcIdx) ++ tail ++ v.drop(srcIdx + 1))
      }
    )
  }

  def commandWithSourcesImpl[Out](
    parts: List[BodyPart],
    codes: Tuple,
    bff: SourceBodyArgsProj[? <: Tuple],
    srcIdx: Int
  ): CommandTemplate[Out] =
    CommandTemplate.mk[Out](withSourcesImpl[Out, Void](parts, Void.codec, codes, bff, srcIdx).fragment)

  def returningImpl[Out, R](
    base: List[BodyPart],
    ret: Fragment[?],
    codec: Codec[R],
    codes: Tuple
  ): QueryTemplate[Out, R] = {
    val parts: List[BodyPart] = base ++ List[BodyPart](SelectBuilder.bake(RawConstants.RETURNING), Right(ret))
    SelectBuilder.assembleN[Out, R](parts, Nil, codec, new skunk.sharp.where.SlotSplit(codes))
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

  inline def where[A](f: JoinedView[Ss] => Where[A]): DeleteUsingReady[Cols, Name, Ss, A] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    new DeleteUsingReady[Cols, Name, Ss, A](table, sources, Some(pred.fragment))
  }

  inline def whereRaw(af: AppliedFragment): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto[Void](None, af, Where.projPair[Void, Void])
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  inline def deleteAll: DeleteUsingReady[Cols, Name, Ss, Void] =
    new DeleteUsingReady[Cols, Name, Ss, Void](table, sources, None)

}

final class DeleteUsingReady[
  Cols <: Tuple,
  Name <: String & Singleton,
  Ss <: Tuple,
  Args
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val whereOpt: Option[Fragment[?]]
) {

  inline def where[A](f: JoinedView[Ss] => Where[A]): DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]] = {
    val view     = buildJoinedView(sources)
    val pred     = f(view)
    val combined = SelectBuilder.andInto[Args, A](
      whereOpt.asInstanceOf[Option[Fragment[Args]]],
      pred,
      Where.projPair[Args, A]
    )
    new DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]](table, sources, Some(combined))
  }

  inline def whereRaw(af: AppliedFragment): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto[Args](
      whereOpt.asInstanceOf[Option[Fragment[Args]]],
      af,
      Where.projPair[Args, Void]
    )
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  /**
   * DELETE … USING <tail sources> WHERE — emits per-source body Right slots so typed-subquery USING sources thread
   * their inner Args into the outer command's args.
   */
  private def bodyParts: List[BodyPart] = {
    val buf          = scala.collection.mutable.ListBuffer[BodyPart](SelectBuilder.bake(table.deleteFromHeader))
    val usingEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].tail
    if (usingEntries.nonEmpty) {
      buf += SelectBuilder.bake(RawConstants.USING)
      var first = true
      usingEntries.foreach { s =>
        if (first) first = false else buf += SelectBuilder.bake(TypedExpr.raw(", "))
        aliasedFromEntryParts(s).foreach(buf += _)
      }
    }
    whereOpt.foreach { f =>
      buf += SelectBuilder.bake(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  // Slots: SArgs (per USING source) ⊕ Args (WHERE).
  inline def compile[SArgs](using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): CommandTemplate[Where.Concat[SArgs, Args]] =
    MutationAssembly.commandWithSourcesImpl[Where.Concat[SArgs, Args]](
      bodyParts,
      Where.slotCodes[(SArgs, Args)],
      bff,
      0
    )

  // Slots: SArgs ⊕ Args (WHERE) ⊕ A (RETURNING).
  inline def returning[T, A, SArgs](f: JoinedView[Ss] => TypedExpr[T, A])(using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[Where.Concat[Where.Concat[SArgs, Args], A], T] = {
    val expr = f(buildJoinedView(sources))
    MutationAssembly.withSourcesImpl[Where.Concat[Where.Concat[SArgs, Args], A], T](
      bodyParts ++ List[BodyPart](Left(TypedExpr.liftAfToVoid(RawConstants.RETURNING)), Right(expr.fragment)),
      expr.codec,
      Where.slotCodes[(SArgs, Args, A)],
      bff,
      0
    )
  }

  inline def returningTuple[T <: NonEmptyTuple, SArgs, TOut](f: JoinedView[Ss] => T)(using
    pa: ProjArgsOf.Aux[T, TOut],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[Where.Concat[Where.Concat[SArgs, Args], TOut], ExprOutputs[T]] = {
    val exprs    = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    returning[ExprOutputs[T], TOut, SArgs](_ =>
      TypedExpr[ExprOutputs[T], TOut](combined, codec)
    )
  }

  inline def returningNamed[NT <: scala.NamedTuple.AnyNamedTuple, SArgs, TOut](f: JoinedView[Ss] => NT)(using
    pa: ProjArgsOf.Aux[scala.NamedTuple.DropNames[NT], TOut],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[
    Where.Concat[Where.Concat[SArgs, Args], TOut],
    scala.NamedTuple.NamedTuple[scala.NamedTuple.Names[NT], ExprOutputs[scala.NamedTuple.DropNames[NT]]]
  ] = {
    type Vs = scala.NamedTuple.DropNames[NT]
    type Ns = scala.NamedTuple.Names[NT]
    type R  = scala.NamedTuple.NamedTuple[Ns, ExprOutputs[Vs]]
    val tup      = f(buildJoinedView(sources)).asInstanceOf[Product]
    val exprs    = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[R]]
    val combined = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    returning[R, TOut, SArgs](_ =>
      TypedExpr[R, TOut](combined, codec)
    )
  }

  inline def returningAll[SArgs](using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[Where.Concat[SArgs, Args], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec    = skunk.sharp.internal.RowCodecs.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    val combined = TypedExpr.combineList[Void](exprs.map(_.fragment), ", ", _ => List.fill(exprs.size)(Void))
    returning[NamedRowOf[Cols], Void, SArgs](_ =>
      TypedExpr[NamedRowOf[Cols], Void](combined, codec)
    ).asInstanceOf[QueryTemplate[Where.Concat[SArgs, Args], NamedRowOf[Cols]]]
  }

}

// ---- BodyPart re-export ---------------------------------------------------------------------------

private[dsl] type BodyPart = SelectBuilder.BodyPart

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols, Name] = new DeleteBuilder[Cols, Name](table)
}
