package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec}
import skunk.sharp.*
import skunk.sharp.internal.tupleCodec
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.{&&, Where}

/**
 * UPDATE builder — compile-time staged so you can't accidentally run a rowset-nuking `UPDATE` with no WHERE.
 *
 * State machine:
 *
 *   1. `users.update` → [[UpdateBuilder]] (entry). Must call `.set(…)` before anything else.
 *   2. `.set(…)` → [[UpdateWithSet]]. No `.run` / `.returning` here — the only paths forward are `.where(…)` or the
 *      explicit `.updateAll` opt-in for "yes, I really mean every row".
 *   3. `.where(…)` or `.updateAll` → [[UpdateReady]]. This is where `.run`, `.returning`, `.returningTuple`,
 *      `.returningAll` live. `.where` here chains (AND-combines).
 *
 * Calling `.run` without `.where` (or `.updateAll`) is a compile error: the method simply does not exist at that state.
 *
 * {{{
 *   users.update
 *     .set(u => (u.email := "new@example.com", u.age := 30))
 *     .where(u => u.id === someId)          // transitions to UpdateReady
 *     .run(session)
 *
 *   users.update.set(u => u.active := false).updateAll.run(session)  // explicit opt-in
 * }}}
 *
 * Future (v0.1+): `FROM`/`USING`, `.set` that accepts a subset named tuple, richer RHS expressions.
 */
final class UpdateBuilder[Cols <: Tuple] private[sharp] (table: Table[Cols, ?]) {

  /**
   * Declare the SET list. Accepts one [[SetAssignment]] or a tuple of them. Must be followed by `.where` or
   * `.updateAll`.
   */
  def set(f: ColumnsView[Cols] => SetAssignment[?] | Tuple): UpdateWithSet[Cols] = {
    val view        = ColumnsView(table.columns)
    val assignments = f(view) match {
      case sa: SetAssignment[?] => List(sa)
      case t: Tuple             => t.toList.asInstanceOf[List[SetAssignment[?]]]
    }
    new UpdateWithSet[Cols](table, assignments)
  }

}

/**
 * State after `.set(…)`, before a WHERE (or explicit `.updateAll`) has been committed. Deliberately has no `.run` or
 * `.returning` — the type forces the caller to narrow the update or to ask for the unrestricted version explicitly.
 */
final class UpdateWithSet[Cols <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, ?],
  private[sharp] val assignments: List[SetAssignment[?]]
) {

  /** Narrow with a WHERE clause. Transitions to [[UpdateReady]]. */
  def where(f: ColumnsView[Cols] => Where): UpdateReady[Cols] = {
    val view = ColumnsView(table.columns)
    new UpdateReady[Cols](table, assignments, Some(f(view)))
  }

  /** "Yes, update every row." Explicit opt-in — skips the WHERE requirement. */
  def updateAll: UpdateReady[Cols] =
    new UpdateReady[Cols](table, assignments, None)

}

/** UPDATE in a runnable state: SET list filled, plus either a WHERE clause or an explicit `.updateAll` opt-in. */
final class UpdateReady[Cols <: Tuple] private[sharp] (
  table: Table[Cols, ?],
  assignments: List[SetAssignment[?]],
  whereOpt: Option[Where]
) {

  /** Chain another WHERE — AND-combined with the existing one. */
  def where(f: ColumnsView[Cols] => Where): UpdateReady[Cols] = {
    val view = ColumnsView(table.columns)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new UpdateReady[Cols](table, assignments, Some(next))
  }

  def compile: CompiledCommand = CompiledCommand(compileFragment)

  private[sharp] def compileFragment: AppliedFragment = {
    val header = TypedExpr.raw(s"UPDATE ${table.qualifiedName} SET ")
    val sets   = TypedExpr.joined(assignments.map(_.render), ", ")
    val base   = header |+| sets
    whereOpt.fold(base)(w => base |+| TypedExpr.raw(" WHERE ") |+| w.render)
  }

  /** Append `RETURNING <expr>` — single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): MutationReturning[T] = {
    val view = ColumnsView(table.columns)
    val expr = f(view)
    new MutationReturning[T](compileFragment, List(expr), expr.codec)
  }

  /** Append `RETURNING <e1>, <e2>, …` — tuple form. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): MutationReturning[ExprOutputs[T]] = {
    val view  = ColumnsView(table.columns)
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new MutationReturning[ExprOutputs[T]](compileFragment, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — whole-row projection (same shape as the table's default SELECT). */
  def returningAll: MutationReturning[NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean, Boolean, Boolean]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new MutationReturning[NamedRowOf[Cols]](compileFragment, exprs, codec)
  }

}

/**
 * Shared `RETURNING` shape used by UPDATE and DELETE. INSERT has its own [[InsertReturning]] because its underlying
 * statement is built per-row (`VALUES (…), (…)`), whereas UPDATE/DELETE are single-statement — the base
 * `AppliedFragment` is reused as-is and we just append `RETURNING …` to it.
 */
final class MutationReturning[R] private[sharp] (
  base: AppliedFragment,
  returning: List[TypedExpr[?]],
  returnCodec: Codec[R]
) {

  def compile: CompiledQuery[R] = {
    val list = TypedExpr.joined(returning.map(_.render), ", ")
    CompiledQuery(base |+| TypedExpr.raw(" RETURNING ") |+| list, returnCodec)
  }

}

/** One `column = expression` assignment in an UPDATE SET list. */
final case class SetAssignment[T](col: TypedColumn[T, ?, ?], expr: TypedExpr[T]) {

  def render: AppliedFragment =
    TypedExpr.raw(s""""${col.name}" = """) |+| expr.render

}

extension [T, Null <: Boolean, N <: String & Singleton](col: TypedColumn[T, Null, N]) {

  /** `col = value` assignment. */
  def :=(value: T)(using pf: PgTypeFor[T]): SetAssignment[T] =
    SetAssignment(col, TypedExpr.lit(value))

  /** `col = <expression>` assignment — right-hand side can be any typed expression (function call, arithmetic, …). */
  def :=(expr: TypedExpr[T]): SetAssignment[T] =
    SetAssignment(col, expr)

}

/** UPDATE entry point lives on [[Table]] (views reject at compile time). `users.update.set(…).where(…)`. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def update: UpdateBuilder[Cols] = new UpdateBuilder[Cols](table)
}
