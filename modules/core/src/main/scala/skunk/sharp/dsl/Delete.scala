package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, RawConstants}
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
    new DeleteUsingBuilder[
      Cols,
      Name,
      SourceEntry[Table[Cols, Name], Cols, Cols, Name] *: SourceEntry[RR, CR, CR, AR] *: EmptyTuple
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
    c123: Where.Concat2[Where.Concat[Args, Void], A]
  ): QueryTemplate[Where.Concat[Args, A], T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturningTyped[Args, Void, A, T](deleteParts, expr.fragment, expr.codec)
      .asInstanceOf[QueryTemplate[Where.Concat[Args, A], T]]
  }

  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): QueryTemplate[Args, ExprOutputs[T]] = {
    val exprs = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, Void, ExprOutputs[T]](deleteParts, exprs, codec).asInstanceOf[QueryTemplate[Args, ExprOutputs[T]]]
  }

  def returningAll: QueryTemplate[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Args, Void, NamedRowOf[Cols]](deleteParts, exprs, codec).asInstanceOf[QueryTemplate[Args, NamedRowOf[Cols]]]
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
    val tpl = SelectBuilder.assemble[A1, A2, Void](parts, Nil, Void.codec)
    CommandTemplate.mk[Where.Concat[A1, A2]](tpl.fragment.asInstanceOf[Fragment[Where.Concat[A1, A2]]])
  }

  def withReturning[A1, A2, R](
    base: List[BodyPart],
    returning: List[TypedExpr[?, ?]],
    codec: Codec[R]
  )(using c2: Where.Concat2[A1, A2]): QueryTemplate[Where.Concat[A1, A2], R] = {
    // Multi-item RETURNING — items individually bind at Void (typed-Args threading per item is roadmap).
    val listAf = TypedExpr.joined(returning.map(e => SelectBuilder.bindVoid(e.fragment)), ", ")
    val parts: List[BodyPart] = base ++ List[BodyPart](Left(RawConstants.RETURNING), Left(listAf))
    SelectBuilder.assemble[A1, A2, R](parts, Nil, codec)
  }

  /**
   * Single-expression RETURNING that threads typed Args. The expression's `RetArgs` becomes the third
   * typed slot at execute time, after SET/WHERE.
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
    SelectBuilder.assemble3[A1, A2, RetArgs, R](parts, Nil, codec)
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
  ): DeleteUsingBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]] = {
    val rel   = aR(other)
    val oCols = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new DeleteUsingBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]](
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

  def compile: CommandTemplate[Args] = MutationAssembly.command[Args, Void](bodyParts).asInstanceOf[CommandTemplate[Args]]

  private def bodyParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(table.deleteFromHeader))
    val usingEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    if (usingEntries.nonEmpty) {
      buf += Left(RawConstants.USING)
      buf += Left(TypedExpr.joined(usingEntries.map(aliasedFromEntry), ", "))
    }
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    buf.toList
  }

  def returning[T, A](f: JoinedView[Ss] => TypedExpr[T, A]): QueryTemplate[Args, T] = {
    val expr = f(buildJoinedView(sources))
    MutationAssembly.withReturning[Args, Void, T](bodyParts, List(expr), expr.codec).asInstanceOf[QueryTemplate[Args, T]]
  }
  // (DELETE … USING.returning keeps the multi-item path; threading typed RetArgs through USING is roadmap.)

  def returningTuple[T <: NonEmptyTuple](f: JoinedView[Ss] => T): QueryTemplate[Args, ExprOutputs[T]] = {
    val exprs = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, Void, ExprOutputs[T]](bodyParts, exprs, codec).asInstanceOf[QueryTemplate[Args, ExprOutputs[T]]]
  }

  def returningAll: QueryTemplate[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Args, Void, NamedRowOf[Cols]](bodyParts, exprs, codec).asInstanceOf[QueryTemplate[Args, NamedRowOf[Cols]]]
  }

}

// ---- BodyPart re-export ---------------------------------------------------------------------------

private[dsl] type BodyPart = SelectBuilder.BodyPart

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols, Name] = new DeleteBuilder[Cols, Name](table)
}
