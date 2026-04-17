package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Session}
import skunk.sharp.*
import skunk.sharp.internal.tupleCodec
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/**
 * UPDATE builder.
 *
 * {{{
 *   update(users)
 *     .set(u => (u.email := "new@example.com", u.age := 30))
 *     .where(u => u.id === someId)
 *     .run(session)
 * }}}
 *
 * `:=` is an extension on [[TypedColumn]] that produces a [[SetAssignment]]. `.set` takes one assignment or a tuple of
 * them. A WHERE clause is effectively mandatory in practice — forgetting it nukes the whole table — but we do not
 * enforce that at the type level (yet). Future (v0.1+): `RETURNING`, `FROM`/`USING`, `.set` that accepts a subset named
 * tuple, SQL-expression right-hand sides.
 */
final class UpdateBuilder[Cols <: Tuple] private[sharp] (table: Table[Cols]) {

  def set(f: ColumnsView[Cols] => SetAssignment[?] | Tuple): UpdateWithSet[Cols] = {
    val view        = ColumnsView(table.columns)
    val assignments = f(view) match {
      case sa: SetAssignment[?] => List(sa)
      case t: Tuple             => t.toList.asInstanceOf[List[SetAssignment[?]]]
    }
    new UpdateWithSet[Cols](table, assignments, None)
  }

}

final class UpdateWithSet[Cols <: Tuple] private[sharp] (
  table: Table[Cols],
  assignments: List[SetAssignment[?]],
  whereOpt: Option[Where]
) {

  def where(f: ColumnsView[Cols] => Where): UpdateWithSet[Cols] = {
    val view = ColumnsView(table.columns)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new UpdateWithSet[Cols](table, assignments, Some(next))
  }

  def compile: AppliedFragment = {
    val header = TypedExpr.raw(s"UPDATE ${table.qualifiedName} SET ")
    val sets   = TypedExpr.joined(assignments.map(_.render), ", ")
    val base   = header |+| sets
    whereOpt.fold(base)(w => base |+| TypedExpr.raw(" WHERE ") |+| w.render)
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

  /** Append `RETURNING <all columns>` — whole-row projection (same shape as the table's default SELECT). */
  def returningAll: MutationReturning[NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    new MutationReturning[NamedRowOf[Cols]](compile, exprs, codec)
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

  def compile: (AppliedFragment, Codec[R]) = {
    val list = TypedExpr.joined(returning.map(_.render), ", ")
    (base |+| TypedExpr.raw(" RETURNING ") |+| list, returnCodec)
  }

  def run[F[_]](session: Session[F]): F[List[R]] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.execute(query)(af.argument)
  }

  /** Run and return exactly one row. */
  def unique[F[_]](session: Session[F]): F[R] = {
    val (af, c) = compile
    val query   = af.fragment.query(c)
    session.unique(query)(af.argument)
  }

}

/** One `column = expression` assignment in an UPDATE SET list. */
final case class SetAssignment[T](col: TypedColumn[T, ?], expr: TypedExpr[T]) {

  def render: AppliedFragment =
    TypedExpr.raw(s""""${col.name}" = """) |+| expr.render

}

extension [T, Null <: Boolean](col: TypedColumn[T, Null]) {

  /** `col = value` assignment. */
  def :=(value: T)(using pf: PgTypeFor[T]): SetAssignment[T] =
    SetAssignment(col, TypedExpr.lit(value))

  /** `col = <expression>` assignment — right-hand side can be any typed expression (function call, arithmetic, …). */
  def :=(expr: TypedExpr[T]): SetAssignment[T] =
    SetAssignment(col, expr)

}

/** UPDATE entry point lives on [[Table]] (views reject at compile time). `users.update.set(…).where(…)`. */
extension [Cols <: Tuple](table: Table[Cols]) {
  def update: UpdateBuilder[Cols] = new UpdateBuilder[Cols](table)
}
