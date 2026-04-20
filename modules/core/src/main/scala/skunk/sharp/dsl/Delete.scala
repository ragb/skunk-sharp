/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 *   1. `users.delete` → [[DeleteBuilder]] (entry). No `.run` / `.returning` here; only `.where(…)` or the explicit
 *      `.deleteAll` opt-in are visible.
 *   2. `.where(…)` or `.deleteAll` → [[DeleteReady]]. `.run`, `.returning`, `.returningTuple`, `.returningAll` live
 *      here. `.where` here chains (AND-combines).
 *
 * Calling `.run` at the initial state is a compile error: the method does not exist on [[DeleteBuilder]].
 *
 * {{{
 *   users.delete.where(u => u.id === someId).run(session)
 *   users.delete.deleteAll.run(session)   // explicit opt-in for "drop everything"
 * }}}
 */
final class DeleteBuilder[Cols <: Tuple] private[sharp] (table: Table[Cols, ?]) {

  /** Narrow with a WHERE clause. Transitions to [[DeleteReady]]. */
  def where(f: ColumnsView[Cols] => Where): DeleteReady[Cols] = {
    val view = ColumnsView(table.columns)
    new DeleteReady[Cols](table, Some(f(view)))
  }

  /** "Yes, delete every row." Explicit opt-in — skips the WHERE requirement. */
  def deleteAll: DeleteReady[Cols] =
    new DeleteReady[Cols](table, None)

}

/** DELETE in a runnable state: WHERE clause committed (or `.deleteAll` explicitly called). */
final class DeleteReady[Cols <: Tuple] private[sharp] (
  table: Table[Cols, ?],
  whereOpt: Option[Where]
) {

  /** Chain another WHERE — AND-combined with the existing one. */
  def where(f: ColumnsView[Cols] => Where): DeleteReady[Cols] = {
    val view = ColumnsView(table.columns)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new DeleteReady[Cols](table, Some(next))
  }

  def compile: CompiledCommand = CompiledCommand(compileFragment)

  private[sharp] def compileFragment: AppliedFragment = {
    val header = TypedExpr.raw(s"DELETE FROM ${table.qualifiedName}")
    whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
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

/** DELETE entry point lives on [[Table]] (views reject at compile time). `users.delete.where(…)`. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def delete: DeleteBuilder[Cols] = new DeleteBuilder[Cols](table)
}
