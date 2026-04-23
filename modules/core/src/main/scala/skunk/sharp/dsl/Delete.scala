package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec}
import skunk.sharp.*
import skunk.sharp.internal.tupleCodec
import skunk.sharp.where.{&&, Where}

/**
 * DELETE builder — compile-time staged so you can't accidentally run a `DELETE FROM …` with no WHERE.
 *
 * State machine:
 *
 *   1. `users.delete` → [[DeleteBuilder]] (entry). No `.run` / `.returning` here; only `.where(…)`, the explicit
 *      `.deleteAll` opt-in, or `.using(…)` for multi-source form are visible.
 *   2. `.where(…)` or `.deleteAll` → [[DeleteReady]]. `.run`, `.returning`, `.returningTuple`, `.returningAll` live
 *      here. `.where` here chains (AND-combines).
 *
 * For multi-table form (`DELETE … USING …`):
 *
 *   1. `.using(otherRel)` on [[DeleteBuilder]] → [[DeleteUsingBuilder]]. Chain further `.using(…)` for more sources.
 *   2. `.where(r => r.<target>…)` / `.deleteAll` → [[DeleteUsingReady]].
 *
 * {{{
 *   users.delete.where(u => u.id === someId).run(session)
 *   users.delete.deleteAll.run(session)   // explicit opt-in for "drop everything"
 *
 *   // Multi-table (DELETE … USING …)
 *   users.delete
 *     .using(posts)
 *     .where(r => r.users.id ==== r.posts.user_id)
 *     .compile.run(session)
 * }}}
 */
final class DeleteBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  /** Narrow with a WHERE clause. Transitions to [[DeleteReady]]. */
  def where(f: ColumnsView[Cols] => Where): DeleteReady[Cols] = {
    val view = table.columnsView
    new DeleteReady[Cols](table, Some(f(view)))
  }

  /** "Yes, delete every row." Explicit opt-in — skips the WHERE requirement. */
  def deleteAll: DeleteReady[Cols] =
    new DeleteReady[Cols](table, None)

  /**
   * Add an extra USING source — transitions to [[DeleteUsingBuilder]], which exposes `.where` with a multi-source
   * `JoinedView` lambda. The resulting SQL is `DELETE FROM <target> USING <other>, … WHERE …`.
   *
   * Chain `.using(a).using(b)` for multiple extra sources. The alias of each added source must be distinct from the
   * target's name and from all previously-added source aliases.
   */
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

/** DELETE in a runnable state: WHERE clause committed (or `.deleteAll` explicitly called). */
final class DeleteReady[Cols <: Tuple] private[sharp] (
  table: Table[Cols, ?],
  whereOpt: Option[Where]
) {

  /** Chain another WHERE — AND-combined with the existing one. */
  def where(f: ColumnsView[Cols] => Where): DeleteReady[Cols] = {
    val view = table.columnsView
    val next = whereOpt.fold(f(view))(_ && f(view))
    new DeleteReady[Cols](table, Some(next))
  }

  def compile: CompiledCommand = CompiledCommand(compileFragment)

  private[sharp] def compileFragment: AppliedFragment = {
    val header = table.deleteFromHeader
    whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
  }

  /** Append `RETURNING <expr>` — single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): MutationReturning[T] = {
    val view = table.columnsView
    val expr = f(view)
    new MutationReturning[T](compileFragment, List(expr), expr.codec)
  }

  /** Append `RETURNING <e1>, <e2>, …` — tuple form. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): MutationReturning[ExprOutputs[T]] = {
    val view  = table.columnsView
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new MutationReturning[ExprOutputs[T]](compileFragment, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — whole-row projection. */
  def returningAll: MutationReturning[NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new MutationReturning[NamedRowOf[Cols]](compileFragment, exprs, codec)
  }

}

// ---- DELETE … USING -------------------------------------------------------------------------------

/**
 * Multi-source DELETE builder — the result of calling `.using(other)` on [[DeleteBuilder]]. Transitions to
 * [[DeleteUsingReady]] after `.where(…)` or `.deleteAll`.
 *
 * `Ss` is the full sources tuple: the target table entry sits at the head (index 0), extra USING sources follow at
 * indices 1+. The `.where` lambda receives `JoinedView[Ss]` keyed by each source's alias.
 *
 * Rendered form: `DELETE FROM "target" USING "src1", "src2", … WHERE …`.
 */
final class DeleteUsingBuilder[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

  /** Chain another USING source. Its alias must be distinct from all aliases already in `Ss`. */
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

  /** Narrow with a WHERE clause. Transitions to [[DeleteUsingReady]]. */
  def where(f: JoinedView[Ss] => Where): DeleteUsingReady[Cols, Name, Ss] = {
    val view = buildJoinedView(sources)
    new DeleteUsingReady[Cols, Name, Ss](table, sources, Some(f(view)))
  }

  /** "Yes, delete every row." Explicit opt-in — skips the WHERE requirement. */
  def deleteAll: DeleteUsingReady[Cols, Name, Ss] =
    new DeleteUsingReady[Cols, Name, Ss](table, sources, None)

}

/**
 * Multi-source DELETE in a runnable state. Renders `DELETE FROM "target" USING "src1", … WHERE …`.
 *
 * `.returning` / `.returningTuple` lambdas receive the full `JoinedView[Ss]` — columns from target and USING sources
 * are all accessible. `.returningAll` returns only the target table's full row.
 */
final class DeleteUsingReady[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  table: Table[Cols, Name],
  sources: Ss,
  whereOpt: Option[Where]
) {

  /** Chain another WHERE — AND-combined with the existing one. */
  def where(f: JoinedView[Ss] => Where): DeleteUsingReady[Cols, Name, Ss] = {
    val view = buildJoinedView(sources)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new DeleteUsingReady[Cols, Name, Ss](table, sources, Some(next))
  }

  def compile: CompiledCommand = CompiledCommand(compileFragment)

  private[sharp] def compileFragment: AppliedFragment = {
    val header       = table.deleteFromHeader
    val usingEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    val withUsing    =
      if usingEntries.isEmpty then header
      else header |+| TypedExpr.raw(" USING ") |+| TypedExpr.joined(usingEntries.map(aliasedFromEntry), ", ")
    whereOpt.fold(withUsing)(w => withUsing |+| TypedExpr.raw(" WHERE ") |+| w.render)
  }

  /** Append `RETURNING <expr>` — lambda receives the full `JoinedView[Ss]`. */
  def returning[T](f: JoinedView[Ss] => TypedExpr[T]): MutationReturning[T] = {
    val view = buildJoinedView(sources)
    val expr = f(view)
    new MutationReturning[T](compileFragment, List(expr), expr.codec)
  }

  /** Append `RETURNING <e1>, <e2>, …` — tuple form, full `JoinedView[Ss]`. */
  def returningTuple[T <: NonEmptyTuple](f: JoinedView[Ss] => T): MutationReturning[ExprOutputs[T]] = {
    val view  = buildJoinedView(sources)
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new MutationReturning[ExprOutputs[T]](compileFragment, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — whole-row projection of the target table only. */
  def returningAll: MutationReturning[NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new MutationReturning[NamedRowOf[Cols]](compileFragment, exprs, codec)
  }

}

// ---- Entry point ----------------------------------------------------------------------------------

/** DELETE entry point lives on [[Table]] (views reject at compile time). `users.delete.where(…)`. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols, Name] = new DeleteBuilder[Cols, Name](table)
}
