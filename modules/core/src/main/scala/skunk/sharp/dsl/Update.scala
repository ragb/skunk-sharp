package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, CompileChecks}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.{&&, Where}
import skunk.util.Origin

import scala.NamedTuple
import scala.compiletime.constValueTuple

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
final class UpdateBuilder[Cols <: Tuple] private[sharp] (private[sharp] val table: Table[Cols, ?]) {

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

  /**
   * Subset-named-tuple UPDATE — each field is `Option[ColumnType]`, and only the `Some` fields hit the SET list. The
   * shape mirrors `insert`'s subset-named-tuple form: field names are a compile-time-checked subset of the table's
   * columns, each value's Scala type must match `Option[<column's declared type>]`. Unlike INSERT, there's no "required
   * columns must be covered" check — that's exactly why `patch` exists.
   *
   *   - `(email = Some("new@x"), age = None)` — set `email`, leave `age` alone.
   *   - For a nullable column (`deleted_at: Option[OffsetDateTime]`), the patch field has type
   *     `Option[Option[OffsetDateTime]]`: `None` = leave alone, `Some(None)` = set to NULL, `Some(Some(ts))` = set to
   *     `ts`.
   *
   * Runtime check: if every field is `None`, throws — an empty SET list is always a mistake, and Postgres would reject
   * the SQL.
   */
  inline def patch[R <: NamedTuple.AnyNamedTuple](p: R): UpdateWithSet[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requirePatchValueTypes[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names  = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val values = p.asInstanceOf[Tuple].toList
    buildPatch(table, names, values)
  }

}

/**
 * Runtime half of [[UpdateBuilder.patch]]. Lives outside the class so it isn't re-generated per inlining. Walks
 * `(name, value)` pairs, drops the `None`s, and turns each `Some(v)` into a [[SetAssignment]] via the column's own
 * codec — no `PgTypeFor` lookup needed (column already carries its codec).
 */
private[sharp] def buildPatch[Cols <: Tuple](
  table: Table[Cols, ?],
  names: List[String],
  values: List[Any]
): UpdateWithSet[Cols] = {
  val allCols                             = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
  val byName                              = allCols.iterator.map(c => c.name.toString -> c).toMap
  val assignments: List[SetAssignment[?]] =
    names.zip(values).collect { case (n, Some(v)) =>
      val col      = byName(n)
      val codec    = col.codec.asInstanceOf[Codec[Any]]
      val rowFrag  = Fragment(List(Right(codec.sql)), codec, Origin.unknown)
      val rendered = rowFrag(v)
      val expr     = TypedExpr(rendered, codec)
      val tc       = TypedColumn.of(col.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      SetAssignment[Any](tc, expr)
    }
  if (assignments.isEmpty)
    throw new IllegalArgumentException(
      "skunk-sharp: .patch(...) produced an empty SET list — every field was None. Postgres rejects UPDATE without SET; provide at least one Some(...)."
    )
  new UpdateWithSet[Cols](table, assignments)
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
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
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
    SetAssignment(col, TypedExpr.parameterised(value))

  /** `col = <expression>` assignment — right-hand side can be any typed expression (function call, arithmetic, …). */
  def :=(expr: TypedExpr[T]): SetAssignment[T] =
    SetAssignment(col, expr)

}

/** UPDATE entry point lives on [[Table]] (views reject at compile time). `users.update.set(…).where(…)`. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def update: UpdateBuilder[Cols] = new UpdateBuilder[Cols](table)
}
