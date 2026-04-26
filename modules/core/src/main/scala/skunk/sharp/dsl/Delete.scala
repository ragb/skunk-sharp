package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, RawConstants}
import skunk.sharp.where.Where

/**
 * DELETE builder — compile-time staged so you can't accidentally run a `DELETE FROM …` with no WHERE, and
 * threads `Args` end-to-end so `.compile` surfaces `CompiledCommand[Args]` (or `CompiledQuery[Args, R]` via
 * `.returning`).
 *
 * State machine:
 *
 *   1. `users.delete` → [[DeleteBuilder]] (entry, `Args = Void`).
 *   2. `.where(_ => Where[A])` → [[DeleteReady]] with `Args = A`. Subsequent `.where` extends `Args` to
 *      `Where.Concat[Args, A2]`.
 *   3. `.deleteAll` → [[DeleteReady]] with `Args = Void`.
 *   4. `.compile`, `.returning*` available on [[DeleteReady]].
 *
 * Multi-table form (`DELETE … USING …`):
 *
 *   1. `.using(otherRel)` → [[DeleteUsingBuilder]].
 *   2. `.where(...)` / `.deleteAll` → [[DeleteUsingReady]].
 */
final class DeleteBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  /** Narrow with a typed WHERE clause. Transitions to [[DeleteReady]] carrying `Args = A`. */
  def where[A](f: ColumnsView[Cols] => Where[A]): DeleteReady[Cols, Name, A] = {
    val pred = f(table.columnsView)
    new DeleteReady[Cols, Name, A](table, Some((pred.fragment, pred.args)))
  }

  /** Escape hatch: AND in a pre-applied `AppliedFragment`. */
  def whereRaw(af: AppliedFragment): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto(None, af)
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  /** "Yes, delete every row." Explicit opt-in — skips the WHERE requirement. */
  def deleteAll: DeleteReady[Cols, Name, Void] =
    new DeleteReady[Cols, Name, Void](table, None)

  /** Add an extra USING source — transitions to [[DeleteUsingBuilder]]. */
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

/** DELETE in a runnable state. Carries the cumulative `Args` from typed WHERE predicates. */
final class DeleteReady[Cols <: Tuple, Name <: String & Singleton, Args] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val whereOpt: Option[(Fragment[?], Any)]
) {

  /** Chain another typed WHERE — AND-combined; extends `Args` via `Where.Concat`. */
  def where[A](f: ColumnsView[Cols] => Where[A]): DeleteReady[Cols, Name, Where.Concat[Args, A]] = {
    val pred = f(table.columnsView)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    new DeleteReady[Cols, Name, Where.Concat[Args, A]](table, Some(combined))
  }

  /** Escape hatch — widens `Args` to `?`. */
  def whereRaw(af: AppliedFragment): DeleteReady[Cols, Name, ?] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    new DeleteReady[Cols, Name, Any](table, Some(combined))
  }

  /** Body-part list for the inner DELETE statement (without RETURNING). */
  private def deleteParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(table.deleteFromHeader))
    whereOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.WHERE)
      buf += Right((f, a))
    }
    buf.toList
  }

  def compile: CompiledCommand[Args] = MutationAssembly.command[Args](deleteParts)

  /** Append `RETURNING <expr>` — single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturning[Args, T](deleteParts, List(expr), expr.codec)
  }

  /** Append `RETURNING <e1>, <e2>, …` — tuple form. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): CompiledQuery[Args, ExprOutputs[T]] = {
    val exprs = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, ExprOutputs[T]](deleteParts, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — whole-row projection. */
  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Args, NamedRowOf[Cols]](deleteParts, exprs, codec)
  }

}

/**
 * Shared command/RETURNING assembly for mutation builders (DELETE / UPDATE / INSERT). All take a body-parts
 * list (the statement up to but not including RETURNING) and either compile it as a `CompiledCommand[Args]` or
 * append `RETURNING <exprs>` and compile as `CompiledQuery[Args, R]`. Args is preserved across the boundary —
 * RETURNING projections don't introduce captures (column refs / aggregates only).
 */
private[dsl] object MutationAssembly {

  def command[Args](parts: List[BodyPart]): CompiledCommand[Args] = {
    val cq = SelectBuilder.assemble[Args, skunk.Void](parts, Nil, skunk.Void.codec)
    CompiledCommand.mk[Args](cq.fragment, cq.args)
  }

  def withReturning[Args, R](
    base: List[BodyPart],
    returning: List[TypedExpr[?]],
    codec: Codec[R]
  ): CompiledQuery[Args, R] = {
    val list  = TypedExpr.joined(returning.map(_.render), ", ")
    val parts = base :+ Left(RawConstants.RETURNING) :+ Left(list)
    SelectBuilder.assemble[Args, R](parts, Nil, codec)
  }

}

// ---- DELETE … USING -------------------------------------------------------------------------------

/**
 * Multi-source DELETE builder — the result of calling `.using(other)` on [[DeleteBuilder]]. Transitions to
 * [[DeleteUsingReady]] after `.where(…)` or `.deleteAll`.
 */
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
    new DeleteUsingReady[Cols, Name, Ss, A](table, sources, Some((pred.fragment, pred.args)))
  }

  def whereRaw(af: AppliedFragment): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto(None, af)
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  def deleteAll: DeleteUsingReady[Cols, Name, Ss, Void] =
    new DeleteUsingReady[Cols, Name, Ss, Void](table, sources, None)

}

/** Multi-source DELETE in a runnable state. */
final class DeleteUsingReady[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, Args] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val whereOpt: Option[(Fragment[?], Any)]
) {

  def where[A](f: JoinedView[Ss] => Where[A]): DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    new DeleteUsingReady[Cols, Name, Ss, Where.Concat[Args, A]](table, sources, Some(combined))
  }

  def whereRaw(af: AppliedFragment): DeleteUsingReady[Cols, Name, Ss, ?] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    new DeleteUsingReady[Cols, Name, Ss, Any](table, sources, Some(combined))
  }

  def compile: CompiledCommand[Args] = MutationAssembly.command[Args](bodyParts)

  private def bodyParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](Left(table.deleteFromHeader))
    val usingEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    if (usingEntries.nonEmpty) {
      buf += Left(RawConstants.USING)
      buf += Left(TypedExpr.joined(usingEntries.map(aliasedFromEntry), ", "))
    }
    whereOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.WHERE)
      buf += Right((f, a))
    }
    buf.toList
  }

  def returning[T](f: JoinedView[Ss] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val expr = f(buildJoinedView(sources))
    MutationAssembly.withReturning[Args, T](bodyParts, List(expr), expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](f: JoinedView[Ss] => T): CompiledQuery[Args, ExprOutputs[T]] = {
    val exprs = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Args, ExprOutputs[T]](bodyParts, exprs, codec)
  }

  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Args, NamedRowOf[Cols]](bodyParts, exprs, codec)
  }

}

// ---- BodyPart re-export ---------------------------------------------------------------------------

/** Local alias for the BodyPart type defined on SelectBuilder — keeps the signatures terse. */
private[dsl] type BodyPart = SelectBuilder.BodyPart

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols, Name] = new DeleteBuilder[Cols, Name](table)
}
