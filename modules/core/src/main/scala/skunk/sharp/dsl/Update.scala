package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{tupleCodec, CompileChecks, RawConstants}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.util.Origin

import scala.NamedTuple
import scala.deriving.Mirror
import scala.compiletime.constValueTuple

/**
 * UPDATE builder — compile-time staged so you can't accidentally run a rowset-nuking `UPDATE` with no WHERE.
 *
 * Threads two captured-args type parameters end-to-end:
 *
 *   - `SetArgs` — the captured-values tuple from the SET assignments.
 *   - `WArgs`   — the cumulative WHERE captured-args tuple.
 *
 * `.compile` produces `CompiledCommand[Where.Concat[SetArgs, WArgs]]`.
 *
 * State machine:
 *
 *   1. `users.update` → [[UpdateBuilder]] (entry).
 *   2. `.set(_ => …)` → [[UpdateWithSet]] carrying `SetArgs`.
 *   3. `.where(_ => Where[A])` → [[UpdateReady]] with `WArgs = A`. `.updateAll` → `WArgs = Void`.
 *   4. `.compile`, `.returning*` available on [[UpdateReady]].
 *
 * Multi-table form (`UPDATE … FROM …`):
 *
 *   1. `.from(otherRel)` on [[UpdateBuilder]] → [[UpdateFromBuilder]].
 *   2. `.set(...)` → [[UpdateFromWithSet]].
 *   3. `.where(...)` / `.updateAll` → [[UpdateFromReady]].
 */
final class UpdateBuilder[Cols <: Tuple, Name <: String & Singleton] private[sharp] (
  private[sharp] val table: Table[Cols, Name]
) {

  /**
   * Declare the SET list. Accepts one [[SetAssignment]] or a tuple of them. Returns [[UpdateWithSet]] with
   * `SetArgs` carrying the captured values from the SET RHS.
   */
  def set(f: ColumnsView[Cols] => SetAssignment[?, ?] | Tuple): UpdateWithSet[Cols, Name, Any] = {
    val view = table.columnsView
    val raw = f(view) match {
      case sa: SetAssignment[?, ?] => List(sa)
      case t: Tuple                => t.toList.asInstanceOf[List[SetAssignment[?, ?]]]
    }
    val combined = SetAssignment.combineAll(raw)
    new UpdateWithSet[Cols, Name, Any](table, combined.fragment, combined.args)
  }

  /**
   * Subset-named-tuple UPDATE — each field is `Option[ColumnType]`, and only the `Some` fields hit the SET
   * list. `SetArgs` is widened to `?` because the actual fields populated are determined at runtime.
   */
  inline def patch[R <: NamedTuple.AnyNamedTuple](p: R): UpdateWithSet[Cols, Name, Any] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requirePatchValueTypes[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names  = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val values = p.asInstanceOf[Tuple].toList
    buildPatch(table, names, values)
  }

  /** Case-class variant of [[patch]]. */
  inline def patch[T <: Product](p: T)(using m: Mirror.ProductOf[T]): UpdateWithSet[Cols, Name, Any] = {
    CompileChecks.requireAllNamesInCols[Cols, m.MirroredElemLabels]
    CompileChecks.requirePatchValueTypes[Cols, m.MirroredElemLabels, m.MirroredElemTypes]
    val names  = constValueTuple[m.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val values = p.productIterator.toList
    buildPatch(table, names, values)
  }

  /** Add an extra FROM source. */
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
 * Runtime half of [[UpdateBuilder.patch]]. Returns a SET-only `UpdateWithSet[?, ?, ?]` because the columns set
 * are decided at runtime from the `Some` fields.
 */
private[sharp] def buildPatch[Cols <: Tuple, Name <: String & Singleton](
  table: Table[Cols, Name],
  names: List[String],
  values: List[Any]
): UpdateWithSet[Cols, Name, Any] = {
  val allCols     = table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
  val byName      = allCols.iterator.map(c => c.name.toString -> c).toMap
  val assignments: List[SetAssignment[?, ?]] =
    names.zip(values).collect { case (n, Some(v)) =>
      val col = byName(n)
      val tc  = TypedColumn.of(col.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      SetAssignment.fromValue[Any](tc, v.asInstanceOf[Any])(using
        new PgTypeFor[Any] { def codec: Codec[Any] = col.codec.asInstanceOf[Codec[Any]] }
      ).asInstanceOf[SetAssignment[?, ?]]
    }
  if (assignments.isEmpty)
    throw new IllegalArgumentException(
      "skunk-sharp: .patch(...) produced an empty SET list — every field was None. Postgres rejects UPDATE without SET; provide at least one Some(...)."
    )
  val combined = SetAssignment.combineAll(assignments)
  new UpdateWithSet[Cols, Name, Any](table, combined.fragment, combined.args)
}

/**
 * State after `.set(…)`. Forces the caller to narrow the update or to ask for the unrestricted version
 * explicitly via `.updateAll`.
 */
final class UpdateWithSet[Cols <: Tuple, Name <: String & Singleton, SetArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val setArgs: Any
) {

  /** Narrow with a typed WHERE clause. */
  def where[A](f: ColumnsView[Cols] => Where[A]): UpdateReady[Cols, Name, SetArgs, A] = {
    val pred = f(table.columnsView)
    new UpdateReady[Cols, Name, SetArgs, A](
      table,
      setFragment,
      setArgs,
      Some((pred.fragment, pred.args))
    )
  }

  /** Escape hatch — widens `WArgs` to `?`. */
  def whereRaw(af: AppliedFragment): UpdateReady[Cols, Name, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto(None, af)
    new UpdateReady[Cols, Name, SetArgs, Any](table, setFragment, setArgs, Some(combined))
  }

  /** Explicit "update every row" — `WArgs = Void`. */
  def updateAll: UpdateReady[Cols, Name, SetArgs, Void] =
    new UpdateReady[Cols, Name, SetArgs, Void](table, setFragment, setArgs, None)

}

/** UPDATE in a runnable state. Args = `Where.Concat[SetArgs, WArgs]`. */
final class UpdateReady[Cols <: Tuple, Name <: String & Singleton, SetArgs, WArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val setArgs: Any,
  private[sharp] val whereOpt: Option[(Fragment[?], Any)]
) {

  /** Chain another typed WHERE — AND-combined; extends `WArgs`. */
  def where[A](f: ColumnsView[Cols] => Where[A]): UpdateReady[Cols, Name, SetArgs, Where.Concat[WArgs, A]] = {
    val pred = f(table.columnsView)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    new UpdateReady[Cols, Name, SetArgs, Where.Concat[WArgs, A]](table, setFragment, setArgs, Some(combined))
  }

  /** Escape hatch — widens `WArgs` to `?`. */
  def whereRaw(af: AppliedFragment): UpdateReady[Cols, Name, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    new UpdateReady[Cols, Name, SetArgs, Any](table, setFragment, setArgs, Some(combined))
  }

  private def updateParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](
      Left(table.updateSetHeader),
      Right((setFragment, setArgs))
    )
    whereOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.WHERE)
      buf += Right((f, a))
    }
    buf.toList
  }

  def compile: CompiledCommand[Where.Concat[SetArgs, WArgs]] =
    MutationAssembly.command[Where.Concat[SetArgs, WArgs]](updateParts)

  /** Append `RETURNING <expr>`. */
  def returning[T](f: ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Where.Concat[SetArgs, WArgs], T] = {
    val expr = f(table.columnsView)
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], T](updateParts, List(expr), expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](
    f: ColumnsView[Cols] => T
  ): CompiledQuery[Where.Concat[SetArgs, WArgs], ExprOutputs[T]] = {
    val exprs = f(table.columnsView).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], ExprOutputs[T]](updateParts, exprs, codec)
  }

  def returningAll: CompiledQuery[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]](updateParts, exprs, codec)
  }

}

// ---- UPDATE … FROM ---------------------------------------------------------------------------------

final class UpdateFromBuilder[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

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

  def set(f: JoinedView[Ss] => SetAssignment[?, ?] | Tuple): UpdateFromWithSet[Cols, Name, Ss, Any] = {
    val view = buildJoinedView(sources)
    val raw = f(view) match {
      case sa: SetAssignment[?, ?] => List(sa)
      case t: Tuple                => t.toList.asInstanceOf[List[SetAssignment[?, ?]]]
    }
    val combined = SetAssignment.combineAll(raw)
    new UpdateFromWithSet[Cols, Name, Ss, Any](table, sources, combined.fragment, combined.args)
  }

}

final class UpdateFromWithSet[Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, SetArgs] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val setArgs: Any
) {

  def where[A](f: JoinedView[Ss] => Where[A]): UpdateFromReady[Cols, Name, Ss, SetArgs, A] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, A](
      table,
      sources,
      setFragment,
      setArgs,
      Some((pred.fragment, pred.args))
    )
  }

  def whereRaw(af: AppliedFragment): UpdateFromReady[Cols, Name, Ss, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto(None, af)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Any](table, sources, setFragment, setArgs, Some(combined))
  }

  def updateAll: UpdateFromReady[Cols, Name, Ss, SetArgs, Void] =
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Void](table, sources, setFragment, setArgs, None)

}

final class UpdateFromReady[
  Cols <: Tuple, Name <: String & Singleton, Ss <: Tuple, SetArgs, WArgs
] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val setFragment: Fragment[?],
  private[sharp] val setArgs: Any,
  private[sharp] val whereOpt: Option[(Fragment[?], Any)]
) {

  def where[A](f: JoinedView[Ss] => Where[A]): UpdateFromReady[Cols, Name, Ss, SetArgs, Where.Concat[WArgs, A]] = {
    val view = buildJoinedView(sources)
    val pred = f(view)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Where.Concat[WArgs, A]](
      table, sources, setFragment, setArgs, Some(combined)
    )
  }

  def whereRaw(af: AppliedFragment): UpdateFromReady[Cols, Name, Ss, SetArgs, ?] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    new UpdateFromReady[Cols, Name, Ss, SetArgs, Any](
      table, sources, setFragment, setArgs, Some(combined)
    )
  }

  private def updateFromParts: List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart](
      Left(table.updateSetHeader),
      Right((setFragment, setArgs))
    )
    val fromEntries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    if (fromEntries.nonEmpty) {
      buf += Left(TypedExpr.raw(" FROM "))
      buf += Left(TypedExpr.joined(fromEntries.map(aliasedFromEntry), ", "))
    }
    whereOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.WHERE)
      buf += Right((f, a))
    }
    buf.toList
  }

  def compile: CompiledCommand[Where.Concat[SetArgs, WArgs]] =
    MutationAssembly.command[Where.Concat[SetArgs, WArgs]](updateFromParts)

  def returning[T](f: JoinedView[Ss] => TypedExpr[T]): CompiledQuery[Where.Concat[SetArgs, WArgs], T] = {
    val expr = f(buildJoinedView(sources))
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], T](updateFromParts, List(expr), expr.codec)
  }

  def returningTuple[T <: NonEmptyTuple](
    f: JoinedView[Ss] => T
  ): CompiledQuery[Where.Concat[SetArgs, WArgs], ExprOutputs[T]] = {
    val exprs = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], ExprOutputs[T]](updateFromParts, exprs, codec)
  }

  def returningAll: CompiledQuery[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]] = {
    val exprs =
      table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c =>
        TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]])
      )
    val codec = skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]
    MutationAssembly.withReturning[Where.Concat[SetArgs, WArgs], NamedRowOf[Cols]](updateFromParts, exprs, codec)
  }

}

// ---- SetAssignment ---------------------------------------------------------------------------------

/**
 * One typed `column = expression` assignment in an UPDATE SET list. Carries `Args` — the captured-values tuple
 * from the assignment's RHS (a single bound value for `col := someValue`, or a Void-ish marker for a `col :=
 * expr` form whose expression doesn't bind anything new).
 */
final class SetAssignment[T, Args] private[sharp] (
  private[sharp] val col:       TypedColumn[T, ?, ?],
  private[sharp] val fragment:  Fragment[Args],
  private[sharp] val args:      Args
)

object SetAssignment {

  /** `col := value` — bound parameter. Args = T. */
  def fromValue[T](col: TypedColumn[T, ?, ?], value: T)(using pf: PgTypeFor[T]): SetAssignment[T, T] = {
    val enc = pf.codec
    val frag: Fragment[T] = Fragment(
      List(Left(s""""${col.name}" = """), Right(enc.sql)),
      enc,
      Origin.unknown
    )
    new SetAssignment[T, T](col, frag, value)
  }

  /**
   * `col := expr` — RHS is an existing TypedExpr (function call, column ref, etc.). Args = Void; the LHS's
   * existing AppliedFragment args are baked via contramap.
   */
  def fromExpr[T](col: TypedColumn[T, ?, ?], expr: TypedExpr[T]): SetAssignment[T, Void] = {
    val af   = expr.render
    val parts = List[Either[String, cats.data.State[Int, String]]](Left(s""""${col.name}" = """)) ++ af.fragment.parts
    val srcEnc: Encoder[Any] = af.fragment.encoder.asInstanceOf[Encoder[Any]]
    val srcArgs: Any         = af.argument
    val voidEnc: Encoder[Void] = srcEnc.contramap[Void](_ => srcArgs)
    val frag: Fragment[Void]   = Fragment(parts, voidEnc, Origin.unknown)
    new SetAssignment[T, Void](col, frag, Void)
  }

  /** Combine a non-empty list of assignments — comma-joined, paired-args. Returns a Fragment[?] + Any pair. */
  private[dsl] case class Combined(fragment: Fragment[?], args: Any)

  private[dsl] def combineAll(items: List[SetAssignment[?, ?]]): Combined = {
    require(items.nonEmpty, "skunk-sharp: cannot combine empty SET list")
    val first = items.head
    items.tail.foldLeft(Combined(first.fragment, first.args)) { (acc, sa) =>
      val parts =
        acc.fragment.parts ++ RawConstants.COMMA_SEP.fragment.parts ++ sa.fragment.parts
      val enc   = acc.fragment.encoder.asInstanceOf[Encoder[Any]].product(sa.fragment.encoder)
      val frag: Fragment[?] = Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
      Combined(frag, (acc.args, sa.args))
    }
  }

  /** Infix `&` combinator — comma-style joining for tuple shapes. */
  extension [T1, A1](lhs: SetAssignment[T1, A1])
    def &[T2, A2](rhs: SetAssignment[T2, A2]): SetAssignment[Unit, (A1, A2)] = {
      val parts =
        lhs.fragment.parts ++ RawConstants.COMMA_SEP.fragment.parts ++ rhs.fragment.parts
      val enc   = lhs.fragment.encoder.product(rhs.fragment.encoder)
      val frag: Fragment[(A1, A2)] = Fragment(parts, enc, Origin.unknown)
      new SetAssignment[Unit, (A1, A2)](
        null.asInstanceOf[TypedColumn[Unit, ?, ?]], frag, (lhs.args, rhs.args)
      )
    }

}

extension [T, Null <: Boolean, N <: String & Singleton](col: TypedColumn[T, Null, N]) {

  /** `col = value` — bound parameter. */
  def :=(value: T)(using pf: PgTypeFor[T]): SetAssignment[T, T] =
    SetAssignment.fromValue(col, value)

  /** `col = <expression>`. */
  def :=(expr: TypedExpr[T]): SetAssignment[T, Void] =
    SetAssignment.fromExpr(col, expr)

}

// ---- Entry point ----------------------------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {
  def update: UpdateBuilder[Cols, Name] = new UpdateBuilder[Cols, Name](table)
}
