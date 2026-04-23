package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, CompileChecks}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.{&&, Where}
import skunk.util.Origin

import scala.NamedTuple
import scala.deriving.Mirror
import scala.compiletime.constValueTuple

/**
 * UPDATE builder — compile-time staged so you can't accidentally run a rowset-nuking `UPDATE` with no WHERE.
 *
 * State machine:
 *
 *   1. `users.update` → [[UpdateBuilder]] (entry). Must call `.set(…)` or `.from(…)` before anything else.
 *   2. `.set(…)` → [[UpdateWithSet]]. No `.run` / `.returning` here — the only paths forward are `.where(…)` or the
 *      explicit `.updateAll` opt-in for "yes, I really mean every row".
 *   3. `.where(…)` or `.updateAll` → [[UpdateReady]]. This is where `.run`, `.returning`, `.returningTuple`,
 *      `.returningAll` live. `.where` here chains (AND-combines).
 *
 * For multi-table form (`UPDATE … FROM …`):
 *
 *   1. `.from(otherRel)` on [[UpdateBuilder]] → [[UpdateFromBuilder]]. Chain further `.from(…)` for more sources.
 *   2. `.set(r => r.<target>.<col> := …)` — lambda receives `JoinedView` keyed by alias.
 *   3. `.where` / `.updateAll` → [[UpdateFromReady]] with `.compile` / `.returning*`.
 *
 * {{{
 *   // Single-table
 *   users.update
 *     .set(u => (u.email := "new@example.com", u.age := 30))
 *     .where(u => u.id === someId)
 *     .compile.run(session)
 *
 *   // Multi-table (UPDATE … FROM …)
 *   users.update
 *     .from(posts)
 *     .set(r => r.users.age := 1)
 *     .where(r => r.users.id ==== r.posts.user_id)
 *     .compile.run(session)
 * }}}
 */
final class UpdateBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  /**
   * Declare the SET list. Accepts one [[SetAssignment]] or a tuple of them. Must be followed by `.where` or
   * `.updateAll`.
   */
  def set(f: ColumnsView[Cols] => SetAssignment[?] | Tuple): UpdateWithSet[Cols] = {
    val view        = table.columnsView
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

  /** Case-class variant of [[patch]]. Fields must all be `Option[T]`; `None` means "leave unchanged". */
  inline def patch[T <: Product](p: T)(using m: Mirror.ProductOf[T]): UpdateWithSet[Cols] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requirePatchValueTypes[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names  = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val values = p.productIterator.toList
    buildPatch(table, names, values)
  }

  /**
   * Add an extra FROM source — transitions to [[UpdateFromBuilder]], which exposes `.set` with a multi-source
   * `JoinedView` lambda. The resulting SQL is `UPDATE <target> SET … FROM <other>, … WHERE …`.
   *
   * Chain `.from(a).from(b)` for multiple extra sources. The alias of each added source must be distinct from the
   * target's name and from all previously-added source aliases — duplicate aliases are a compile error.
   */
  def from[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, Name *: EmptyTuple]
  ): UpdateFromBuilder[
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
    new UpdateFromBuilder[
      Cols,
      Name,
      SourceEntry[Table[Cols, Name], Cols, Cols, Name] *: SourceEntry[RR, CR, CR, AR] *: EmptyTuple
    ](table, targetEntry *: otherEntry *: EmptyTuple)
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
    val view = table.columnsView
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
    val view = table.columnsView
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

// ---- UPDATE … FROM ---------------------------------------------------------------------------------

/**
 * Multi-source UPDATE builder — the result of calling `.from(other)` on [[UpdateBuilder]]. Transitions to
 * [[UpdateFromWithSet]] after `.set(…)`.
 *
 * `Ss` is the full sources tuple: the target table entry sits at the head (index 0), extra FROM sources follow at
 * indices 1+. The `.set` / `.where` lambdas receive `JoinedView[Ss]` — a Scala 3 named tuple keyed by each source's
 * alias, so columns are reached as `r.<alias>.<col>`.
 *
 * Rendered form: `UPDATE "target" SET … FROM "src1", "src2", … WHERE …`.
 */
final class UpdateFromBuilder[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

  /** Chain another FROM source. Its alias must be distinct from all aliases already in `Ss`. */
  def from[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](other: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): UpdateFromBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]] = {
    val rel   = aR(other)
    val oCols = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](rel, aR.aliasValue(other), oCols, oCols, JoinKind.Inner, None)
    new UpdateFromBuilder[Cols, Name, Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]](
      table,
      sources :* entry
    )
  }

  /**
   * Declare the SET list. The lambda receives a `JoinedView[Ss]` — columns from all sources are accessible as
   * `r.<alias>.<col>`. The LHS of each `:=` must be a column from the target table; the RHS can reference any source.
   */
  def set(f: JoinedView[Ss] => SetAssignment[?] | Tuple): UpdateFromWithSet[Cols, Name, Ss] = {
    val view        = buildJoinedView(sources)
    val assignments = f(view) match {
      case sa: SetAssignment[?] => List(sa)
      case t: Tuple             => t.toList.asInstanceOf[List[SetAssignment[?]]]
    }
    new UpdateFromWithSet[Cols, Name, Ss](table, sources, assignments)
  }

}

/**
 * State after `.from(…).set(…)` — SET list committed, waiting for WHERE or explicit `.updateAll`.
 */
final class UpdateFromWithSet[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val assignments: List[SetAssignment[?]]
) {

  /** Narrow with a WHERE clause. Transitions to [[UpdateFromReady]]. */
  def where(f: JoinedView[Ss] => Where): UpdateFromReady[Cols, Name, Ss] = {
    val view = buildJoinedView(sources)
    new UpdateFromReady[Cols, Name, Ss](table, sources, assignments, Some(f(view)))
  }

  /** "Yes, update every row." Explicit opt-in — skips the WHERE requirement. */
  def updateAll: UpdateFromReady[Cols, Name, Ss] =
    new UpdateFromReady[Cols, Name, Ss](table, sources, assignments, None)

}

/**
 * Multi-source UPDATE in a runnable state. Renders `UPDATE "target" SET … FROM "src1", … WHERE …`.
 *
 * `.returning` / `.returningTuple` lambdas receive the full `JoinedView[Ss]` — columns from all sources (target and
 * FROM) are accessible. `.returningAll` returns only the target table's full row.
 */
final class UpdateFromReady[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  table: Table[Cols, Name],
  sources: Ss,
  assignments: List[SetAssignment[?]],
  whereOpt: Option[Where]
) {

  /** Chain another WHERE — AND-combined with the existing one. */
  def where(f: JoinedView[Ss] => Where): UpdateFromReady[Cols, Name, Ss] = {
    val view = buildJoinedView(sources)
    val next = whereOpt.fold(f(view))(_ && f(view))
    new UpdateFromReady[Cols, Name, Ss](table, sources, assignments, Some(next))
  }

  def compile: CompiledCommand = CompiledCommand(compileFragment)

  private[sharp] def compileFragment: AppliedFragment = {
    val header      = TypedExpr.raw(s"UPDATE ${table.qualifiedName} SET ")
    val sets        = TypedExpr.joined(assignments.map(_.render), ", ")
    val base        = header |+| sets
    val fromEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    val withFrom    =
      if fromEntries.isEmpty then base
      else base |+| TypedExpr.raw(" FROM ") |+| TypedExpr.joined(fromEntries.map(aliasedFromEntry), ", ")
    whereOpt.fold(withFrom)(w => withFrom |+| TypedExpr.raw(" WHERE ") |+| w.render)
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

// ---- Shared RETURNING shape -----------------------------------------------------------------------

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

  /**
   * Map the returned row to a case class. Works for both plain-tuple projections (from `.returningTuple`) and
   * named-tuple projections (from `.returningAll`): the [[MutationReturning.Unwrap]] match type strips names before the
   * `MirroredElemTypes` comparison so the compiler can resolve the constraint in both cases.
   */
  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = MutationReturning.Unwrap[R] & Tuple }
  ): MutationReturning[T] = {
    val newCodec = returnCodec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[R]
    )
    new MutationReturning[T](base, returning, newCodec)
  }

}

object MutationReturning {

  /**
   * Strip named-tuple labels so `to[T]` accepts both `.returningTuple` (plain tuple) and `.returningAll` (named tuple).
   */
  type Unwrap[R] = R match
    case scala.NamedTuple.NamedTuple[?, v] => v
    case _                                 => R

}

// ---- SetAssignment + := extension -----------------------------------------------------------------

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

// ---- Entry point ----------------------------------------------------------------------------------

/** UPDATE entry point lives on [[Table]] (views reject at compile time). `users.update.set(…).where(…)`. */
extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def update: UpdateBuilder[Cols, Name] = new UpdateBuilder[Cols, Name](table)
}
