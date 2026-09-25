package skunk.sharp.dsl

import skunk.{Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{CompileChecks, RawConstants, RowCodecs}, RowCodecs.tupleCodec
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
  inline def on[A](f: JoinedView[Ss] => Where[A]): MergeCommand[Cols, Name, CR, Ss, A, Void, false, EmptyTuple] =
    new MergeCommand[Cols, Name, CR, Ss, A, Void, false, EmptyTuple](
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
  Ready <: Boolean,
  Closed <: Tuple
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val table: Table[Cols, Name],
  private[sharp] val sources: Ss,
  private[sharp] val onFragment: Fragment[OnArgs],
  private[sharp] val clauses: Fragment[CArgs]
) {

  // Branches of one kind after an unconditional branch of that kind can never fire — Postgres rejects them
  // ("unreachable WHEN clause"). `Closed` lists the kinds already closed that way; each entry point checks it.

  /** `WHEN MATCHED THEN …` — unconditional; no further `whenMatched` branch may follow. */
  inline def whenMatched: MergeMatched[Cols, Name, CR, Ss, OnArgs, CArgs, Void, Closed, true] = {
    Merge.requireOpen[Closed, Merge.Matched]
    new MergeMatched(this, TypedExpr.voidFragment("WHEN MATCHED"))
  }

  /** `WHEN MATCHED AND <cond> THEN …` — `cond` sees both target and source. */
  inline def whenMatched[A](f: JoinedView[Ss] => Where[A])
    : MergeMatched[Cols, Name, CR, Ss, OnArgs, CArgs, A, Closed, false] = {
    Merge.requireOpen[Closed, Merge.Matched]
    new MergeMatched(this, TypedExpr.wrap("WHEN MATCHED AND ", f(buildJoinedView(sources)).fragment, ""))
  }

  /** `WHEN NOT MATCHED THEN …` — a source row with no target row. Unconditional; no further one may follow. */
  inline def whenNotMatched: MergeNotMatched[Cols, Name, CR, Ss, OnArgs, CArgs, Void, Closed, true] = {
    Merge.requireOpen[Closed, Merge.NotMatched]
    new MergeNotMatched(this, TypedExpr.voidFragment("WHEN NOT MATCHED"))
  }

  /** `WHEN NOT MATCHED AND <cond> THEN …` — `cond` sees only the source row. */
  inline def whenNotMatched[A](f: ColumnsView[CR] => Where[A])
    : MergeNotMatched[Cols, Name, CR, Ss, OnArgs, CArgs, A, Closed, false] = {
    Merge.requireOpen[Closed, Merge.NotMatched]
    new MergeNotMatched(this, TypedExpr.wrap("WHEN NOT MATCHED AND ", f(Merge.sourceView[CR](sources)).fragment, ""))
  }

  /** `WHEN NOT MATCHED BY SOURCE THEN …` (PG 17+) — a target row with no source row. Unconditional. */
  inline def whenNotMatchedBySource: MergeBySource[Cols, Name, CR, Ss, OnArgs, CArgs, Void, Closed, true] = {
    Merge.requireOpen[Closed, Merge.BySource]
    new MergeBySource(this, TypedExpr.voidFragment("WHEN NOT MATCHED BY SOURCE"))
  }

  /** `WHEN NOT MATCHED BY SOURCE AND <cond> THEN …` (PG 17+) — `cond` sees only the target row. */
  inline def whenNotMatchedBySource[A](
    f: ColumnsView[Cols] => Where[A]
  ): MergeBySource[Cols, Name, CR, Ss, OnArgs, CArgs, A, Closed, false] = {
    Merge.requireOpen[Closed, Merge.BySource]
    new MergeBySource(
      this,
      TypedExpr.wrap("WHEN NOT MATCHED BY SOURCE AND ", f(Merge.targetView(table)).fragment, "")
    )
  }

  /** Append a finished `WHEN … THEN …` branch. */
  private[sharp] inline def addClause[X, C2 <: Tuple](clause: Fragment[X])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, X], true, C2] =
    new MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[CArgs, X], true, C2](
      table,
      sources,
      onFragment,
      TypedExpr.combineSepInl[CArgs, X](clauses, " ", clause)
    )

  // Header / keyword parts are lifted with `liftAfToVoid` directly rather than the inline `SelectBuilder.bake`: inlining
  // `bake` here makes this file depend on an inline accessor in `SelectBuilder$` that a clean build doesn't emit
  // (NoSuchMethodError at runtime).
  private def mergeParts: List[SelectBuilder.BodyPart] = {
    val source = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]](1)
    List[SelectBuilder.BodyPart](Left(TypedExpr.liftAfToVoid(table.mergeIntoHeader))) ++
      aliasedFromEntryParts(source) ++
      List[SelectBuilder.BodyPart](Left(TypedExpr.liftAfToVoid(RawConstants.ON)), Right(onFragment), Right(clauses))
  }

  private def returningParts(ret: Fragment[?]): List[SelectBuilder.BodyPart] =
    mergeParts ++ List[SelectBuilder.BodyPart](Left(TypedExpr.liftAfToVoid(RawConstants.RETURNING)), Right(ret))

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
  CondA,
  Closed <: Tuple,
  U <: Boolean
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?, Closed],
  private[sharp] val cond: Fragment[CondA]
) {

  /**
   * `THEN UPDATE SET <col := expr>` — both relations are readable, but only target columns can be assigned (and not
   * generated ones); assigning a source column is a compile error.
   */
  inline def update[A](f: MergeSetView[Ss] => SetAssignment[?, A])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
      CArgs,
      Where.Concat[CondA, A]
    ], true, Merge.CloseIf[U, Merge.Matched, Closed]] = {
    val sa = f(buildJoinedView(cmd.sources).asInstanceOf[MergeSetView[Ss]])
    cmd.addClause(TypedExpr.combineSepInl[CondA, A](cond, " THEN UPDATE SET ", sa.fragment))
  }

  /** `THEN UPDATE SET a = …, b = …` — tuple form; Args are the flat fold of every assignment's Args. */
  @scala.annotation.targetName("updateTuple")
  inline def update[T <: Tuple](f: MergeSetView[Ss] => T)
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
      CArgs,
      Where.Concat[CondA, Where.FoldConcat[SetArgsOf[T]]]
    ], true, Merge.CloseIf[U, Merge.Matched, Closed]] =
    cmd.addClause(
      TypedExpr.combineSepInl[CondA, Where.FoldConcat[SetArgsOf[T]]](
        cond,
        " THEN UPDATE SET ",
        SetAssignment.combineTyped[Where.FoldConcat[SetArgsOf[T]]](
          f(buildJoinedView(cmd.sources).asInstanceOf[MergeSetView[Ss]]),
          c => Where.projectFoldConcat[SetArgsOf[T]](c)
        )
      )
    )

  /** `THEN DELETE` — delete the matched target row. */
  inline def delete: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    CondA
  ], true, Merge.CloseIf[U, Merge.Matched, Closed]] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DELETE"))

  /** `THEN DO NOTHING`. */
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    CondA
  ], true, Merge.CloseIf[U, Merge.Matched, Closed]] =
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
  CondA,
  Closed <: Tuple,
  U <: Boolean
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?, Closed],
  private[sharp] val cond: Fragment[CondA]
) {

  /**
   * `THEN INSERT (<cols>) VALUES (<exprs>)` — `f` sees only the source row and returns a named tuple of expressions
   * keyed by target column. Same compile-time checks as `table.insert`: every name is a target column, every required
   * column is present, no generated column is written, and each expression's type fits its column.
   */
  inline def insert[R <: NamedTuple.AnyNamedTuple, TOut](f: ColumnsView[CR] => R)(using
    pa: ProjArgsOf.Aux[NamedTuple.DropNames[R], TOut]
  ): MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    Where.Concat[CondA, TOut]
  ], true, Merge.CloseIf[U, Merge.NotMatched, Closed]] = {
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
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    CondA
  ], true, Merge.CloseIf[U, Merge.NotMatched, Closed]] =
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
  CondA,
  Closed <: Tuple,
  U <: Boolean
] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val cmd: MergeCommand[Cols, Name, CR, Ss, OnArgs, CArgs, ?, Closed],
  private[sharp] val cond: Fragment[CondA]
) {

  /** `THEN UPDATE SET <col := expr>` — only the target row is visible. */
  inline def update[A](f: SetView[Cols] => SetAssignment[?, A])
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
      CArgs,
      Where.Concat[CondA, A]
    ], true, Merge.CloseIf[U, Merge.BySource, Closed]] = {
    val sa = f(Merge.targetView(cmd.table).asInstanceOf[SetView[Cols]])
    cmd.addClause(TypedExpr.combineSepInl[CondA, A](cond, " THEN UPDATE SET ", sa.fragment))
  }

  /** `THEN UPDATE SET a = …, b = …` — tuple form; Args are the flat fold of every assignment's Args. */
  @scala.annotation.targetName("updateTuple")
  inline def update[T <: Tuple](f: SetView[Cols] => T)
    : MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
      CArgs,
      Where.Concat[CondA, Where.FoldConcat[SetArgsOf[T]]]
    ], true, Merge.CloseIf[U, Merge.BySource, Closed]] =
    cmd.addClause(
      TypedExpr.combineSepInl[CondA, Where.FoldConcat[SetArgsOf[T]]](
        cond,
        " THEN UPDATE SET ",
        SetAssignment.combineTyped[Where.FoldConcat[SetArgsOf[T]]](
          f(Merge.targetView(cmd.table).asInstanceOf[SetView[Cols]]),
          c => Where.projectFoldConcat[SetArgsOf[T]](c)
        )
      )
    )

  /** `THEN DELETE` — delete the target row that has no source row. */
  inline def delete: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    CondA
  ], true, Merge.CloseIf[U, Merge.BySource, Closed]] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DELETE"))

  /** `THEN DO NOTHING`. */
  inline def doNothing: MergeCommand[Cols, Name, CR, Ss, OnArgs, Where.Concat[
    CArgs,
    CondA
  ], true, Merge.CloseIf[U, Merge.BySource, Closed]] =
    cmd.addClause(TypedExpr.wrap("", cond, " THEN DO NOTHING"))

}

/** The value type of a `TypedExpr` (a column, `Param`, function call, …). */
type ExprValue[X] = X match {
  case TypedExpr[t, ?] => t
}

/**
 * The view a `whenMatched` SET lambda receives: the target as a [[skunk.sharp.SetView]] (assignable, generated columns
 * excepted), the source as a read-only [[skunk.sharp.SourceView]].
 */
type MergeSetView[Ss <: Tuple] = Ss match {
  case SourceEntry[?, ?, ct, at, ?] *: SourceEntry[?, ?, cs, as, ?] *: EmptyTuple =>
    NamedTuple.NamedTuple[at *: as *: EmptyTuple, SetView[ct] *: SourceView[cs] *: EmptyTuple]
}

/**
 * Evidence that expression type `V` can be written into a column of Scala type `C` (named `N`): `V` is a
 * `TypedExpr[C, ?]`, or `C = Option[X]` and `V` is a `TypedExpr[X, ?]`.
 */
@scala.annotation.implicitNotFound(
  "skunk-sharp: MERGE INSERT value for column ${N} doesn't match the column's type ${C} (the expression is ${V})"
)
sealed trait ExprFits[V, C, N]

object ExprFits {
  private val instance: ExprFits[Any, Any, Any] = new ExprFits[Any, Any, Any] {}

  given exact[V, C, N](using V <:< TypedExpr[C, ?]): ExprFits[V, C, N] = instance.asInstanceOf[ExprFits[V, C, N]]

  given intoNullable[V, X, N](using V <:< TypedExpr[X, ?]): ExprFits[V, Option[X], N] =
    instance.asInstanceOf[ExprFits[V, Option[X], N]]

}

object Merge {

  // Branch kinds, as singletons so the error message can name them.
  type Matched    = "WHEN MATCHED"
  type NotMatched = "WHEN NOT MATCHED"
  type BySource   = "WHEN NOT MATCHED BY SOURCE"

  /** `Closed` after a branch of kind `K`: an unconditional branch (`U = true`) closes its kind. */
  type CloseIf[U <: Boolean, K, Closed <: Tuple] <: Tuple = U match {
    case true  => K *: Closed
    case false => Closed
  }

  /** A branch of kind `K` is unreachable once an unconditional branch of that kind has been added. */
  inline def requireOpen[Closed <: Tuple, K <: String & Singleton]: Unit =
    inline if constValue[skunk.sharp.Contains[K, Closed]] then
      error(
        "skunk-sharp: unreachable " + constValue[K] + " branch — an earlier unconditional " + constValue[K] +
          " already catches every such row. Add a condition to the earlier branch, or drop this one."
      )
    else ()

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
   * for a nullable column, either `Option[X]` or `X`. Evidence is [[ExprFits]], whose `@implicitNotFound` names the
   * column. (`summonInline`, not `summonFrom`: the latter's type patterns accept any `v` here.)
   */
  inline def requireExprTypesMatch[Cols <: Tuple, Ns <: Tuple, Vs <: Tuple]: Unit =
    inline erasedValue[Ns] match {
      case _: EmptyTuple => ()
      case _: (n *: nt)  =>
        inline erasedValue[Vs] match {
          case _: (v *: vt) =>
            summonInline[ExprFits[v, ColumnType[Cols, n & String & Singleton], n]]
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
