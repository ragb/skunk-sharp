package skunk.sharp.dsl

import skunk.{Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{CompileChecks, RawConstants, RowCodecs}, RowCodecs.tupleCodec
import skunk.sharp.ops.Stripped
import skunk.sharp.where.Where

import scala.NamedTuple
import scala.compiletime.{constValue, constValueTuple, erasedValue, error, summonInline}

/**
 * `MERGE INTO <target> USING <source> ON <cond> WHEN … THEN …` (PG 15+; `WHEN NOT MATCHED BY SOURCE` and `RETURNING`
 * need PG 17+).
 *
 * {{{
 *   stock
 *     .merge(incoming)
 *     .on(r => r.stock.sku === r.incoming.sku)
 *     .whenMatched(r => r.incoming.qty === lit(0)).delete
 *     .whenMatched.update(r => r.stock.qty := r.incoming.qty)
 *     .whenNotMatched.insert(s => (sku = s.sku, qty = s.qty))
 *     .compile
 * }}}
 *
 * Each branch sees what Postgres lets it see: `whenMatched` gets both relations (`r.<target>` / `r.<source>`),
 * `whenNotMatched` only the source row, `whenNotMatchedBySource` only the target row. Branches are tried in order and
 * the first match wins, as in SQL. `.compile` exists only after at least one `WHEN` branch.
 *
 * Args: the source body (typed subquery sources) ⊕ `ON` ⊕ each branch's condition and action, in SQL order.
 */
final class MergeBuilder[Cols <: Tuple, Name <: String & Singleton, CR <: Tuple, Ss <: Tuple] private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss
) {

  /** `ON <cond>` — the join between target and source. Required before any `WHEN` branch. */
  inline def on[A](f: JoinedView[Ss] => Where[A]): MergeCommand[Cols, Name, CR, Ss, A, Void, false] =
    new MergeCommand[Cols, Name, CR, Ss, A, Void, false](
      table,
      sources,
      f(buildJoinedView(sources)).fragment,
      SelectBuilder.emptyVoidSlot
    )

}

/**
 * A MERGE with its `ON` clause and zero or more `WHEN` branches. `CArgs` is the Args of every branch so far (flat, via
 * `Where.Concat`); `Ready` is `true` once there's at least one branch, which is what `.compile` / `.returning` require.
 */
final class MergeCommand[
  Cols <: Tuple,
  Name <: String & Singleton,
  CR <: Tuple,
  Ss <: Tuple,
  OnArgs,
  CArgs,
  Ready <: Boolean
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val onFragment: Fragment[OnArgs],
  private[sharp] val clauses: Fragment[CArgs]
) {

  /** `WHEN MATCHED THEN …` — unconditional. */
  def whenMatched: MergeMatched[Cols, Name, CR, Ss, OnArgs, CArgs, Void] =
    new MergeMatched(this, TypedExpr.voidFragment("WHEN MATCHED"))

  /** `WHEN MATCHED AND <cond> THEN …` — `cond` sees both target and source. */
  inline def whenMatched[A](f: JoinedView[Ss] => Where[A]): MergeMatched[Cols, Name, CR, Ss, OnArgs, CArgs, A] =
    new MergeMatched(this, TypedExpr.wrap("WHEN MATCHED AND ", f(buildJoinedView(sources)).fragment, ""))

  /** `WHEN NOT MATCHED THEN …` — a source row with no target row. Unconditional. */
  def whenNotMatched: MergeNotMatched[Cols, Name, CR, Ss, OnArgs, CArgs, Void] =
    new MergeNotMatched(this, TypedExpr.voidFragment("WHEN NOT MATCHED"))

  /** `WHEN NOT MATCHED AND <cond> THEN …` — `cond` sees only the source row. */
  inline def whenNotMatched[A](f: ColumnsView[CR] => Where[A]): MergeNotMatched[Cols, Name, CR, Ss, OnArgs, CArgs, A] =
    new MergeNotMatched(this, TypedExpr.wrap("WHEN NOT MATCHED AND ", f(Merge.sourceView[CR](sources)).fragment, ""))

  /** `WHEN NOT MATCHED BY SOURCE THEN …` (PG 17+) — a target row with no source row. Unconditional. */
  def whenNotMatchedBySource: MergeBySource[Cols, Name, CR, Ss, OnArgs, CArgs, Void] =
    new MergeBySource(this, TypedExpr.voidFragment("WHEN NOT MATCHED BY SOURCE"))

  /** `WHEN NOT MATCHED BY SOURCE AND <cond> THEN …` (PG 17+) — `cond` sees only the target row. */
  inline def whenNotMatchedBySource[A](
    f: ColumnsView[Cols] => Where[A]
  ): MergeBySource[Cols, Name, CR, Ss, OnArgs, CArgs, A] =
    new MergeBySource(
      this,
      TypedExpr.wrap("WHEN NOT MATCHED BY SOURCE AND ", f(Merge.targetView(table)).fragment, "")
    )

  /** Append a finished `WHEN … THEN …` branch. */
  private[sharp] inline def addClause[X](clause: Fragment[X])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, X], true] =
    new MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, X], true](
      table,
      sources,
      onFragment,
      TypedExpr.combineSepInl[CArgs, X](clauses, " ", clause)
    )

  private def mergeParts: List[SelectBuilder.BodyPart] = {
    val source = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]](1)
    List[SelectBuilder.BodyPart](SelectBuilder.bake(table.mergeIntoHeader)) ++
      aliasedFromEntryParts(source) ++
      List[SelectBuilder.BodyPart](SelectBuilder.bake(RawConstants.ON), Right(onFragment), Right(clauses))
  }

  private def returningParts(ret: Fragment[?]): List[SelectBuilder.BodyPart] =
    mergeParts ++ List[SelectBuilder.BodyPart](SelectBuilder.bake(RawConstants.RETURNING), Right(ret))

  /** The source's body Args (a typed subquery source), skipping the target at index 0 — always `Void`. */
  private def sourceBodyArgs(bff: SourceBodyArgsProj[? <: Tuple], sArgs: Any): Any =
    bff.project(sArgs) match {
      case _ :: body :: _ => body
      case _              => Void
    }

  // Concat-chain: SArgs ⊕ OnArgs ⊕ CArgs.
  inline def compile[SArgs](using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): CommandTemplate[Where.Concat[Where.Concat[SArgs, OnArgs], CArgs]] = {
    Merge.requireReady[Ready]
    type Out = Where.Concat[Where.Concat[SArgs, OnArgs], CArgs]
    val slotValues: Out => IArray[Any] = args => {
      val (sOn, cArgs)    = Where.projectConcat[Where.Concat[SArgs, OnArgs], CArgs](args)
      val (sArgs, onArgs) = Where.projectConcat[SArgs, OnArgs](sOn)
      IArray(sourceBodyArgs(bff, sArgs), onArgs, cArgs)
    }
    val tpl = SelectBuilder.assembleN[Out, Void](mergeParts, Nil, Void.codec, slotValues)
    CommandTemplate.mk[Out](tpl.fragment)
  }

  /**
   * `… RETURNING <expr>` (PG 17+). `f` sees both target and source; use [[skunk.sharp.Pg.mergeAction]] to get
   * `'INSERT'` / `'UPDATE'` / `'DELETE'` per row. Source columns are NULL for `NOT MATCHED BY SOURCE` rows.
   */
  inline def returning[T, A, SArgs](f: JoinedView[Ss] => TypedExpr[T, A])(using
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[SArgs, OnArgs], CArgs], A], T] = {
    Merge.requireReady[Ready]
    val expr = f(buildJoinedView(sources))
    type Out = Where.Concat[Where.Concat[Where.Concat[SArgs, OnArgs], CArgs], A]
    val slotValues: Out => IArray[Any] = args => {
      val (sOnC, retArgs) = Where.projectConcat[Where.Concat[Where.Concat[SArgs, OnArgs], CArgs], A](args)
      val (sOn, cArgs)    = Where.projectConcat[Where.Concat[SArgs, OnArgs], CArgs](sOnC)
      val (sArgs, onArgs) = Where.projectConcat[SArgs, OnArgs](sOn)
      IArray(sourceBodyArgs(bff, sArgs), onArgs, cArgs, retArgs)
    }
    SelectBuilder.assembleN[Out, T](returningParts(expr.fragment), Nil, expr.codec, slotValues)
  }

  /** `… RETURNING <e1>, <e2>, …` (PG 17+) — tuple form of [[returning]]. */
  inline def returningTuple[T <: NonEmptyTuple, SArgs, TOut](f: JoinedView[Ss] => T)(using
    pa: ProjArgsOf.Aux[T, TOut],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    bff: SourceBodyArgsProj[Ss]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[SArgs, OnArgs], CArgs], TOut], ExprOutputs[T]] = {
    val exprs    = f(buildJoinedView(sources)).toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec    = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[T]]]
    val combined = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    returning[ExprOutputs[T], TOut, SArgs](_ => TypedExpr[ExprOutputs[T], TOut](combined, codec))
  }

}

/** After `.whenMatched…` — pick the action: `.update(…)`, `.delete`, `.doNothing`. */
final class MergeMatched[
  Cols <: Tuple,
  Name <: String & Singleton,
  CR <: Tuple,
  Ss <: Tuple,
  OnArgs,
  CArgs,
  CondA
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?],
  private[sharp] val cond: Fragment[CondA]
) {

  /** `THEN UPDATE SET <col := expr>` — both relations are visible; generated target columns can't be assigned. */
  inline def update[A](f: SetJoinedView[Ss] => SetAssignment[?, A])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, Where.Concat[CondA, A]], true] = {
    val sa = f(buildJoinedView(cmd.sources).asInstanceOf[SetJoinedView[Ss]])
    cmd.addClause(TypedExpr.combineSepInl[CondA, A](cond, " THEN UPDATE SET ", sa.fragment))
  }

  /** `THEN UPDATE SET a = …, b = …` — tuple form; the assignments' Args collapse to `Void` (use `&` to keep them). */
  @scala.annotation.targetName("updateTuple")
  inline def update(f: SetJoinedView[Ss] => Tuple)
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, Where.Concat[CondA, Void]], true] = {
    val raw = f(buildJoinedView(cmd.sources).asInstanceOf[SetJoinedView[Ss]]).toList
      .asInstanceOf[List[SetAssignment[?, ?]]]
    val set = SetAssignment.combineAll(raw).asInstanceOf[Fragment[Void]]
    cmd.addClause(TypedExpr.combineSepInl[CondA, Void](cond, " THEN UPDATE SET ", set))
  }

  /** `THEN DELETE` — delete the matched target row. */
  inline def delete: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, CondA], true] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DELETE"))

  /** `THEN DO NOTHING`. */
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, CondA], true] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DO NOTHING"))

}

/** After `.whenNotMatched…` — pick the action: `.insert(…)`, `.doNothing`. */
final class MergeNotMatched[
  Cols <: Tuple,
  Name <: String & Singleton,
  CR <: Tuple,
  Ss <: Tuple,
  OnArgs,
  CArgs,
  CondA
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?],
  private[sharp] val cond: Fragment[CondA]
) {

  /**
   * `THEN INSERT (<cols>) VALUES (<exprs>)` — `f` sees only the source row and returns a named tuple of expressions
   * keyed by target column. Same compile-time checks as `table.insert`: every name is a target column, every required
   * column is present, no generated column is written, and each expression's type fits its column.
   */
  inline def insert[R <: NamedTuple.AnyNamedTuple, TOut](f: ColumnsView[CR] => R)(using
    pa: ProjArgsOf.Aux[NamedTuple.DropNames[R], TOut]
  ): MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, Where.Concat[CondA, TOut]], true] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireNoneGenerated[Cols, NamedTuple.Names[R]]
    Merge.requireExprTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names  = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val exprs  = f(Merge.sourceView[CR](cmd.sources)).asInstanceOf[Tuple].toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val values = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    val action = TypedExpr.wrap(s"INSERT (${names.map(n => s""""$n"""").mkString(", ")}) VALUES (", values, ")")
    cmd.addClause(TypedExpr.combineSepInl[CondA, TOut](cond, " THEN ", action))
  }

  /** `THEN DO NOTHING`. */
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, CondA], true] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DO NOTHING"))

}

/** After `.whenNotMatchedBySource…` (PG 17+) — pick the action: `.update(…)`, `.delete`, `.doNothing`. */
final class MergeBySource[
  Cols <: Tuple,
  Name <: String & Singleton,
  CR <: Tuple,
  Ss <: Tuple,
  OnArgs,
  CArgs,
  CondA
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?],
  private[sharp] val cond: Fragment[CondA]
) {

  /** `THEN UPDATE SET <col := expr>` — only the target row is visible. */
  inline def update[A](f: SetView[Cols] => SetAssignment[?, A])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, Where.Concat[CondA, A]], true] = {
    val sa = f(Merge.targetView(cmd.table).asInstanceOf[SetView[Cols]])
    cmd.addClause(TypedExpr.combineSepInl[CondA, A](cond, " THEN UPDATE SET ", sa.fragment))
  }

  /** `THEN UPDATE SET a = …, b = …` — tuple form; the assignments' Args collapse to `Void` (use `&` to keep them). */
  @scala.annotation.targetName("updateTuple")
  inline def update(f: SetView[Cols] => Tuple)
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, Where.Concat[CondA, Void]], true] = {
    val raw = f(Merge.targetView(cmd.table).asInstanceOf[SetView[Cols]]).toList.asInstanceOf[List[SetAssignment[?, ?]]]
    val set = SetAssignment.combineAll(raw).asInstanceOf[Fragment[Void]]
    cmd.addClause(TypedExpr.combineSepInl[CondA, Void](cond, " THEN UPDATE SET ", set))
  }

  /** `THEN DELETE` — delete the target row that has no source row. */
  inline def delete: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, CondA], true] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DELETE"))

  /** `THEN DO NOTHING`. */
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, CondA], true] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DO NOTHING"))

}

/** The value type of a `TypedExpr` (a column, `Param`, function call, …). */
type ExprValue[X] = X match {
  case TypedExpr[t, ?] => t
}

object Merge {

  /** The source row's columns, qualified by the source alias. */
  private[sharp] def sourceView[CR <: Tuple](sources: Tuple): ColumnsView[CR] = {
    val s = sources.toList(1).asInstanceOf[SourceEntry[?, ?, ?, ?, ?]]
    ColumnsView.qualified(s.effectiveCols, s.alias).asInstanceOf[ColumnsView[CR]]
  }

  /** The target row's columns, qualified by the target table's name. */
  private[sharp] def targetView[Cols <: Tuple](table: Table[Cols, ?]): ColumnsView[Cols] =
    ColumnsView.qualified(table.columns, table.currentAlias)

  inline def requireReady[Ready <: Boolean]: Unit =
    inline if constValue[Ready] then ()
    else error("skunk-sharp: MERGE needs at least one WHEN branch (.whenMatched / .whenNotMatched / …) before .compile")

  /**
   * Each INSERT expression's value type must fit its target column: exactly the column's type for a NOT NULL column;
   * for a nullable column, either `Option[X]` or `X`. (`summonInline`, not `summonFrom`: the latter's type patterns
   * accept any `v` here.)
   */
  inline def requireExprTypesMatch[Cols <: Tuple, Ns <: Tuple, Vs <: Tuple]: Unit =
    inline erasedValue[Ns] match {
      case _: EmptyTuple => ()
      case _: (n *: nt)  =>
        inline erasedValue[Vs] match {
          case _: (v *: vt) =>
            inline if constValue[ColumnNullable[Cols, n & String & Singleton]] then
              summonInline[Stripped[ExprValue[v]] <:< Stripped[ColumnType[Cols, n & String & Singleton]]]
            else summonInline[ExprValue[v] <:< ColumnType[Cols, n & String & Singleton]]
            requireExprTypesMatch[Cols, nt, vt]
        }
    }

}

extension [Cols <: Tuple, Name <: String & Singleton](table: Table[Cols, Name]) {

  /** `MERGE INTO <table> USING <source> …` — `source` is a table, view, alias, or aliased subquery. */
  def merge[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](source: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, Name *: EmptyTuple]
  ): MergeBuilder[
    Cols,
    Name,
    CR,
    SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void] *: SourceEntry[RR, CR, CR, AR, Void] *: EmptyTuple
  ] = {
    val targetEntry =
      new SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void](
        table,
        table.currentAlias,
        table.columns,
        table.columns,
        JoinKind.Inner,
        None
      )
    val rel         = aR(source)
    val sCols       = rel.columns.asInstanceOf[CR]
    val sourceEntry =
      new SourceEntry[RR, CR, CR, AR, Void](rel, aR.aliasValue(source), sCols, sCols, JoinKind.Inner, None)
    new MergeBuilder[
      Cols,
      Name,
      CR,
      SourceEntry[Table[Cols, Name], Cols, Cols, Name, Void] *: SourceEntry[RR, CR, CR, AR, Void] *: EmptyTuple
    ](table, targetEntry *: sourceEntry *: EmptyTuple)
  }

}
