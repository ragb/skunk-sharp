package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Session}
import skunk.sharp.*
import skunk.sharp.internal.tupleCodec
import skunk.sharp.where.Where

/**
 * DELETE builder.
 *
 * {{{
 *   delete.from(users).where(u => u.id === someId).run(session)
 * }}}
 *
 * `.where` is optional at the type level but essentially required in practice (no WHERE means "delete all rows"). A
 * future helper like `.deleteAll` could make "delete everything" an explicit opt-in.
 */
final class DeleteBuilder[Cols <: Tuple] private[sharp] (
  table: Table[Cols],
  whereOpt: Option[Where]
) {

  def where(f: ColumnsView[Cols] => Where): DeleteBuilder[Cols] = {
    val view = ColumnsView(table.columns)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new DeleteBuilder[Cols](table, Some(next))
  }

  def compile: AppliedFragment = {
    val header = TypedExpr.raw(s"DELETE FROM ${table.qualifiedName}")
    whereOpt.fold(header)(w => header |+| TypedExpr.raw(" WHERE ") |+| w.render)
  }

  def run[F[_]](session: Session[F]): F[skunk.data.Completion] = {
    val af  = compile
    val cmd = af.fragment.command
    session.execute(cmd)(af.argument)
  }

  /** Append `RETURNING <expr>` — single-value form. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): MutationReturning[T] = {
    val view = ColumnsView(table.columns)
    val expr = f(view)
    new MutationReturning[T](compile, List(expr), expr.codec)
  }

  /** Append `RETURNING <e1>, <e2>, …` — tuple form. */
  def returningTuple[T <: NonEmptyTuple](f: ColumnsView[Cols] => T): MutationReturning[ExprOutputs[T]] = {
    val view  = ColumnsView(table.columns)
    val exprs = f(view).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    new MutationReturning[ExprOutputs[T]](compile, exprs, codec)
  }

  /** Append `RETURNING <all columns>` — whole-row projection. */
  def returningAll: MutationReturning[NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new MutationReturning[NamedRowOf[Cols]](compile, exprs, codec)
  }

}

/** DELETE entry point lives on [[Table]] (views reject at compile time). `users.delete.where(…)`. */
extension [Cols <: Tuple](table: Table[Cols]) {
  def delete: DeleteBuilder[Cols] = new DeleteBuilder[Cols](table, None)
}
