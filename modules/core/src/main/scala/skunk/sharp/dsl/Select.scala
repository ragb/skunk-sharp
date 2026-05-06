package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{RawConstants, RowCodecs}, RowCodecs.{rowCodec, tupleCodec}
import skunk.sharp.where.Where
import skunk.util.Origin

/**
 * Unified SELECT builder — one class for single-source, JOINed, or CROSS-joined queries. Threads two
 * captured-args type parameters end-to-end:
 *
 *   - `WArgs` is the cumulative WHERE args tuple. Starts at `skunk.Void` (no WHERE captured), grows via
 *     `Where.Concat[WArgs, A]` as each `.where(_ => Where[A])` lambda contributes more bound parameters.
 *   - `HArgs` is the cumulative HAVING args tuple. Same shape, threaded by `.having(...)` calls.
 *
 * `.compile` produces a `QueryTemplate[Where.Concat[WArgs, HArgs], Row]` — the visible `Args` is the full
 * captured-parameter tuple in SQL render order (WHERE args first, HAVING args second), with `Void`
 * placeholders normalised away by the [[Where.Concat]] match type.
 */
final class SelectBuilder[Ss <: Tuple, Groups <: Tuple, WArgs, HArgs] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean = false,
  private[sharp] val whereOpt: Option[Fragment[?]] = None,
  private[sharp] val groupBys: List[TypedExpr[?, ?]] = Nil,
  private[sharp] val havingOpt: Option[Fragment[?]] = None,
  private[sharp] val orderBys: List[OrderBy[?]] = Nil,
  private[sharp] val limitOpt: Option[Int] = None,
  private[sharp] val offsetOpt: Option[Int] = None,
  private[sharp] val lockingOpt: Option[Locking] = None,
  private[sharp] val distinctOnOpt: Option[List[TypedExpr[?, ?]]] = None
) {

  private def cp[W, H](
    distinct: Boolean = distinct,
    whereOpt: Option[Fragment[?]] = whereOpt,
    groupBys: List[TypedExpr[?, ?]] = groupBys,
    havingOpt: Option[Fragment[?]] = havingOpt,
    orderBys: List[OrderBy[?]] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt,
    distinctOnOpt: Option[List[TypedExpr[?, ?]]] = distinctOnOpt
  ): SelectBuilder[Ss, Groups, W, H] =
    new SelectBuilder[Ss, Groups, W, H](
      sources,
      distinct,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )

  private def view: SelectView[Ss] = buildSelectView[Ss](sources)

  /** AND in a typed predicate — `WArgs` extends via `Where.Concat`. */
  inline def where[A](f: SelectView[Ss] => Where[A]): SelectBuilder[Ss, Groups, Where.Concat[WArgs, A], HArgs] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred, c => Where.projectConcat[WArgs, A](c))
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  /** Escape hatch — widens `WArgs` to `?`. */
  inline def whereRaw(af: AppliedFragment): SelectBuilder[Ss, Groups, ?, HArgs] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af, c => Where.projectConcat[WArgs, Void](c))
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  /** `ORDER BY …` — typed exprs may carry their own Args; absorbed into the assembled fragment encoder. */
  def orderBy(f: SelectView[Ss] => OrderBy[?] | Tuple): SelectBuilder[Ss, Groups, WArgs, HArgs] = {
    val fresh = (f(view): Any) match {
      case ob: OrderBy[?] => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy[?]]]
    }
    cp[WArgs, HArgs](orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY …` on a pre-projection builder. `Groups` accumulates the projection shape via
   * `Tuple.Concat[Groups, NormProj[G]]`, so a downstream `.select` carries the typed `GArgs` through
   * to `ProjectedSelect.compile` (the runtime size-fallback that handled the old `Groups = EmptyTuple`
   * path is no longer needed for this shape).
   */
  transparent inline def groupBy[G](inline f: SelectView[Ss] => G)
    : SelectBuilder[Ss, Tuple.Concat[Groups, NormProj[G]], WArgs, HArgs] = {
    val v     = view
    val fresh = (f(v): Any) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    new SelectBuilder[Ss, Tuple.Concat[Groups, NormProj[G]], WArgs, HArgs](
      sources,
      distinct,
      whereOpt,
      groupBys ++ fresh,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )
  }

  /** `HAVING <typed-predicate>`. */
  inline def having[H](f: SelectView[Ss] => Where[H]): SelectBuilder[Ss, Groups, WArgs, Where.Concat[HArgs, H]] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[HArgs, H](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], pred, c => Where.projectConcat[HArgs, H](c))
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  /** Escape hatch HAVING — widens `HArgs` to `?`. */
  inline def havingRaw(af: AppliedFragment): SelectBuilder[Ss, Groups, WArgs, ?] = {
    val combined = SelectBuilder.andRawInto[HArgs](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], af, c => Where.projectConcat[HArgs, Void](c))
    cp[WArgs, Any](havingOpt = Some(combined))
  }

  def limit(n: Int): SelectBuilder[Ss, Groups, WArgs, HArgs]  = cp[WArgs, HArgs](limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[Ss, Groups, WArgs, HArgs] = cp[WArgs, HArgs](offsetOpt = Some(n))

  def distinctRows: SelectBuilder[Ss, Groups, WArgs, HArgs] = cp[WArgs, HArgs](distinct = true)

  /** `SELECT DISTINCT ON (e1, e2, …) …`. */
  def distinctOn(f: SelectView[Ss] => TypedExpr[?, ?] | Tuple): SelectBuilder[Ss, Groups, WArgs, HArgs] = {
    val exprs = (f(view): Any) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    cp[WArgs, HArgs](distinctOnOpt = Some(exprs))
  }

  // ---- Row-level locking (single-source Table only) ---------------------------------------------

  def forUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, Groups, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  // ---- Attach more sources (upgrade single-source → multi-source) -------------------------------

  def innerJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, CR, AR, Ss] = {
    val rel  = a(next)
    val cols = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(sources, rel, a.aliasValue(next), cols, cols, JoinKind.Inner)
  }

  def leftJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, NullableCols[CR], AR, Ss] = {
    val rel          = a(next)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(sources, rel, a.aliasValue(next), origCols, effectiveCls, JoinKind.Left)
  }

  def rightJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, CR, AR, NullabilifySources[Ss]] = {
    val rel  = a(next)
    val cols = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(sources, rel, a.aliasValue(next), cols, cols, JoinKind.Right)
  }

  def fullJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, NullableCols[CR], AR, NullabilifySources[Ss]] = {
    val rel          = a(next)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(sources, rel, a.aliasValue(next), origCols, effectiveCls, JoinKind.Full)
  }

  def crossJoin[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    next: T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]], Groups, WArgs, HArgs] = {
    val rel   = a(next)
    val cols  = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR, Void](rel, a.aliasValue(next), cols, cols, JoinKind.Cross, None)
    val next2 = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]], Groups, WArgs, HArgs](
      next2,
      distinct,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  // ---- LATERAL joins ---------------------------------------------------------------------------

  def innerJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: SelectView[Ss] => T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, CR, AR, Ss] = {
    val t    = fn(view)
    val rel  = a(t)
    val cols = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(sources, rel, a.aliasValue(t), cols, cols, JoinKind.Inner, isLateral = true)
  }

  def leftJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: SelectView[Ss] => T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): IncompleteJoin[Ss, RR, CR, NullableCols[CR], AR, Ss] = {
    val t            = fn(view)
    val rel          = a(t)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(sources, rel, a.aliasValue(t), origCols, effectiveCls, JoinKind.Left, isLateral = true)
  }

  def crossJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: SelectView[Ss] => T
  )(using
    a: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AliasesOf[Ss]]
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]], Groups, WArgs, HArgs] = {
    val t     = fn(view)
    val rel   = a(t)
    val cols  = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR, Void](
      rel,
      a.aliasValue(t),
      cols,
      cols,
      JoinKind.Cross,
      None,
      isLateral = true
    )
    val next2 = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR, Void]], Groups, WArgs, HArgs](
      next2,
      distinct,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  // ---- Projection -------------------------------------------------------------------------------

  /**
   * SELECT projection. Dispatches at compile time on the user's projection shape via
   * `compiletime.erasedValue`:
   *
   *   - **single `TypedExpr`** (`u => u.email` or `u => Pg.power(u.age, Param[Double])`): `Proj` becomes
   *     `X *: EmptyTuple`, `Row` is the expression's value type.
   *   - **named tuple** (`u => (email = u.email, age = u.age)`): `Proj` is the underlying value tuple
   *     (via [[scala.NamedTuple.DropNames]]), `Row` is the named tuple of the projected values.
   *   - **plain tuple** (`u => (u.id, u.email)`): `Proj` is `X & Tuple`, `Row` is `ExprOutputs[X]`.
   *
   * `erasedValue` lets us discriminate `NamedTuple` vs regular `Tuple` vs single `TypedExpr` cleanly —
   * the equivalent match-type discriminator hits Scala 3.8's NamedTuple-vs-Tuple disjointness blocker.
   *
   * `Proj` becomes a concrete type per branch, so `compile()`'s `ProjArgsOf[Proj]` summon resolves and
   * Param-bearing projections thread their Args into the QueryTemplate's user-visible `Args`.
   */
  transparent inline def select[X](inline f: SelectView[Ss] => X) = {
    val v = view
    inline scala.compiletime.erasedValue[X] match {
      case _: TypedExpr[?, ?] =>
        val expr = f(v).asInstanceOf[TypedExpr[?, ?]]
        new ProjectedSelect[Ss, X *: EmptyTuple, Groups, EmptyTuple, EmptyTuple, WArgs, HArgs, ProjResult[X]](
          sources,
          distinct,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]],
          whereOpt,
          groupBys,
          havingOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt,
          distinctOnOpt
        )
      case _: scala.NamedTuple.AnyNamedTuple =>
        val tup   = f(v).asInstanceOf[Product]
        val exprs = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
        val codec = tupleCodec(exprs.map(_.codec))
          .asInstanceOf[Codec[scala.NamedTuple.NamedTuple[
            scala.NamedTuple.Names[X & scala.NamedTuple.AnyNamedTuple],
            ExprOutputs[scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple]]
          ]]]
        new ProjectedSelect[
          Ss,
          scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple],
          Groups,
          EmptyTuple,
          EmptyTuple,
          WArgs,
          HArgs,
          scala.NamedTuple.NamedTuple[
            scala.NamedTuple.Names[X & scala.NamedTuple.AnyNamedTuple],
            ExprOutputs[scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple]]
          ]
        ](
          sources,
          distinct,
          exprs,
          codec,
          whereOpt,
          groupBys,
          havingOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt,
          distinctOnOpt
        )
      case _: NonEmptyTuple =>
        val tup   = f(v).asInstanceOf[NonEmptyTuple]
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?, ?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[X & Tuple]]]
        new ProjectedSelect[Ss, X & Tuple, Groups, EmptyTuple, EmptyTuple, WArgs, HArgs, ExprOutputs[X & Tuple]](
          sources,
          distinct,
          exprs,
          codec,
          whereOpt,
          groupBys,
          havingOpt,
          orderBys,
          limitOpt,
          offsetOpt,
          lockingOpt,
          distinctOnOpt
        )
    }
  }

  /** Same as [[select]]; supports `users.select(u => …)` syntax via apply. */
  transparent inline def apply[X](
    @scala.annotation.unused inline f: SelectView[Ss] => X
  ) = select[X](f)

  /**
   * Whole-row `.compile` — only on single-source builders. Threads `SArgs` (from any inner
   * subquery body), `WArgs` (WHERE), `GArgs` (GROUP BY via [[ProjArgsOf]]), and `HArgs`
   * (HAVING) in render order: `[SArgs, WArgs, GArgs, HArgs]`.
   *
   * For plain table / view sources `SArgs = Void` and the return type reduces to the same
   * `QueryTemplate[Concat[Concat[WArgs, GArgs], HArgs], Row]` as before this change.
   */
  // Concat-chain evidences are named after the accumulator they peel from. `cs` = (CArgs, SArgs); `csw` =
  // ((CArgs, SArgs), WArgs); etc. The chain matches the slot order at runtime: the slotValues lambda
  // unfolds outermost-first.
  inline def compile[SArgs, GArgs, CArgs](using
    ev:      IsSingleSource[Ss],
    sbOf:    SourceBodyArgsOf.Aux[Ss, SArgs],
    cteSum:  CteArgsOf.Aux[Ss, CArgs],
    cteProj: CteArgsProj[Ss],
    g:       ProjArgsOf.Aux[Groups, GArgs]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, SArgs], WArgs], GArgs], HArgs], NamedRowOf[ev.Cols]] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
    val head    = entries.head
    val ctes    = collectCtesInOrder(entries)
    val rawGroupProjector = g.project.asInstanceOf[Any => List[Any]]
    val groupProjector: Any => List[Any] = a => {
      val xs = rawGroupProjector(a)
      if (xs.size == groupBys.size) xs else List.fill(groupBys.size)(Void)
    }
    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, SArgs], WArgs], GArgs], HArgs]
    val slotValues: Out => IArray[Any] = args => {
      val (cswgAcc, hArgs) = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[CArgs, SArgs], WArgs], GArgs], HArgs](args)
      val (cswAcc, gArgs)  = Where.projectConcat[Where.Concat[Where.Concat[CArgs, SArgs], WArgs], GArgs](cswgAcc)
      val (csAcc, wArgs)   = Where.projectConcat[Where.Concat[CArgs, SArgs], WArgs](cswAcc)
      val (cArgs, sArgs)   = Where.projectConcat[CArgs, SArgs](csAcc)
      buildCteAndSlotIArrayWithEntries(entries, cteProj, cArgs, ctes, IArray[Any](sArgs, wArgs, gArgs, hArgs))
    }
    SelectBuilder.assembleN[Out, NamedRowOf[ev.Cols]](
      bodyParts  = compileBodyParts(head, groupProjector),
      ctes       = ctes,
      codec      = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]],
      slotValues = slotValues
    )
  }

  /**
   * Produces a typed `Fragment[CombinedArgs]` for the SELECT body **without** any CTE preamble.
   * Used by [[SelectBuilder.alias]] to capture the inner query fragment and surface its `Args` in
   * the outer `BodyArgs` of the resulting subquery relation.
   *
   * Slot order: `[SArgs=0, WArgs=1, GArgs=2, HArgs=3]`.
   */
  private[dsl] inline def compileBodyFragment[SArgs, GArgs](using
    ev:   IsSingleSource[Ss],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    g:    ProjArgsOf.Aux[Groups, GArgs]
  ): Fragment[Where.Concat[Where.Concat[Where.Concat[SArgs, WArgs], GArgs], HArgs]] = {
    // No CTE preamble emitted here — `compileBodyFragment` is used by `.alias` (subquery body) and `cte()`
    // (CTE body) to capture the SELECT body alone. CTEs collected at this nesting level surface in the outer
    // query's preamble. Direct CteRelation refs in this body still bind at FROM-site as Void (their args
    // belong to the outer query's WITH preamble slot, not this body's args).
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
    val head    = entries.head
    val rawGroupProjector = g.project.asInstanceOf[Any => List[Any]]
    val groupProjector: Any => List[Any] = a => {
      val xs = rawGroupProjector(a)
      if (xs.size == groupBys.size) xs else List.fill(groupBys.size)(Void)
    }
    type Out = Where.Concat[Where.Concat[Where.Concat[SArgs, WArgs], GArgs], HArgs]
    val slotValues: Out => IArray[Any] = args => {
      val (swgAcc, hArgs) = Where.projectConcat[Where.Concat[Where.Concat[SArgs, WArgs], GArgs], HArgs](args)
      val (swAcc, gArgs)  = Where.projectConcat[Where.Concat[SArgs, WArgs], GArgs](swgAcc)
      val (sArgs, wArgs)  = Where.projectConcat[SArgs, WArgs](swAcc)
      IArray[Any](sArgs, wArgs, gArgs, hArgs)
    }
    SelectBuilder.assembleN[Out, NamedRowOf[ev.Cols]](
      bodyParts  = compileBodyParts(head, groupProjector),
      ctes       = Nil,
      codec      = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]],
      slotValues = slotValues
    ).fragment
  }

  /**
   * Build body parts for this SELECT. Slot 0 is always the source-body slot:
   *   - typed subquery source: `Left("(") Right(innerFrag) Left(") AS alias")` — the Right carries `BodyArgs`.
   *   - plain source: `Left(fromFragment) Right(emptyVoidSlot)` — the Right is a Void placeholder.
   *   - FROM-less source: `Right(emptyVoidSlot)` immediately.
   *
   * Slots 1 / 2 / 3 are WHERE / GROUP BY / HAVING respectively.
   */
  private def compileBodyParts(
    head: SourceEntry[?, ?, ?, ?, ?],
    groupProjector: Any => List[Any]
  ): List[SelectBuilder.BodyPart] = {
    val rel          = head.relation
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val buf          = scala.collection.mutable.ListBuffer[BodyPart]()
    buf += SelectBuilder.bake(selectPrefix)
    // The projection list (`"col1", "col2"`) depends only on column **names**, which are preserved by
    // `nullabilifyCols` (LEFT/RIGHT/FULL JOIN) and unchanged by re-aliasing. So `rel.starProjAf` is always
    // safe regardless of `effectiveCols` identity or the source's alias. The only thing the alias gates is
    // whether we can use the combined `starProjFromAfOpt` cache — that bakes "FROM <qualifiedName>" with no
    // `AS "alias"`, valid only when alias == relation name.
    val aliasMatchesName = head.alias == rel.currentAlias
    if (rel.hasFromClause) {
      if (aliasMatchesName && (head.effectiveCols eq rel.columns)) {
        rel.starProjFromAfOpt match {
          case Some(af) =>
            // Pre-cached "cols FROM table" string (plain source only — typed subquery overrides to None).
            buf += SelectBuilder.bake(af)
            buf += Right(SelectBuilder.emptyVoidSlot) // slot 0 placeholder (plain source → Void)
          case None =>
            buf += SelectBuilder.bake(rel.starProjAf)
            buf += SelectBuilder.bake(RawConstants.FROM)
            // slot 0: typed subquery → Right(innerFrag); plain → Left(fromFrag) Right(emptyVoidSlot)
            aliasedFromEntryParts(head).foreach(buf += _)
        }
      } else {
        // alias != name OR effectiveCols was nullabilified — projection list still uses cached starProjAf
        // (column names match), but the FROM rendering needs the explicit `AS "alias"` form via
        // `aliasedFromEntryParts`.
        buf += SelectBuilder.bake(rel.starProjAf)
        buf += SelectBuilder.bake(RawConstants.FROM)
        aliasedFromEntryParts(head).foreach(buf += _)
      }
    } else {
      buf += SelectBuilder.bake(rel.starProjAf)
      buf += Right(SelectBuilder.emptyVoidSlot) // slot 0 (FROM-less → Void)
    }
    // No tail-source emission here: every caller of `compileBodyParts` (compile, compileBodyFragment)
    // requires `IsSingleSource[Ss]`, so `Ss = SourceEntry[…] *: EmptyTuple` and there's no tail.
    // Multi-source SELECT goes through `ProjectedSelect.compileBodyParts` instead, which has its own
    // tail-source emission via `aliasedFromEntryParts` and per-source ON Right slots.
    // slot 1 = WHERE
    whereOpt match {
      case Some(f) =>
        buf += SelectBuilder.bake(RawConstants.WHERE)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 2 = GROUP BY
    if (groupBys.nonEmpty) {
      val combinedGrp = TypedExpr.combineList[Any](groupBys.map(_.fragment), ", ", groupProjector)
      buf += SelectBuilder.bake(RawConstants.GROUP_BY)
      buf += Right(combinedGrp)
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 3 = HAVING
    havingOpt match {
      case Some(f) =>
        buf += SelectBuilder.bake(RawConstants.HAVING)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    if (orderBys.nonEmpty) {
      buf += SelectBuilder.bake(RawConstants.ORDER_BY)
      buf += SelectBuilder.bake(TypedExpr.joined(orderBys.map(o => SelectBuilder.bindVoid(o.fragment)), ", "))
    }
    limitOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += SelectBuilder.bake(TypedExpr.raw(" " + l.sql)))
    buf.toList
  }

}

object SelectBuilder {

  // ---- AND-into helpers (typed and raw) --------------------------------------------------------

  /**
   * `proj` re-pairs the `Where.Concat[Slot, A]` runtime value back into `(Slot, A)` for the combined product
   * encoder's contramap — call site materialises it as `c => Where.projectConcat[Slot, A](c)` where `Slot`/`A`
   * are concrete (so the inline dispatch reduces). Passing as a parameter keeps `andInto` itself non-inline
   * — otherwise every chained `.where(...)` would need to be inline too.
   */
  private[dsl] def andInto[Slot, A](
    slot: Option[Fragment[Slot]], pred: Where[A], proj: Where.Concat[Slot, A] => (Slot, A)
  ): Fragment[Where.Concat[Slot, A]] =
    slot match {
      case None    => pred.fragment.asInstanceOf[Fragment[Where.Concat[Slot, A]]]
      case Some(f) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            pred.fragment.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = TypedExpr.combineEnc[Slot, A](f.encoder, pred.fragment.encoder, proj)
        Fragment(parts, enc, Origin.unknown)
    }

  /**
   * Prepend a static SQL prefix to a typed fragment, preserving its `Args` type. Used to attach `" ON "` (or
   * similar static keywords) to a typed predicate fragment so the combined unit can still be threaded through
   * `assembleN`'s typed-slot machinery rather than baked at Void.
   */
  private[dsl] def prefixedFrag[A](prefix: AppliedFragment, body: Fragment[A]): Fragment[A] = {
    val parts = prefix.fragment.parts ++ body.parts
    Fragment(parts, body.encoder, Origin.unknown)
  }

  /** AND a pre-applied raw `AppliedFragment` into a slot — bakes its args via contramap (treats raw as Void-args). */
  private[dsl] def andRawInto[Slot](
    slot: Option[Fragment[Slot]], af: AppliedFragment, proj: Where.Concat[Slot, Void] => (Slot, Void)
  ): Fragment[Where.Concat[Slot, Void]] = {
    val rawFrag: Fragment[Void] = TypedExpr.liftAfToVoid(af)
    slot match {
      case None    => rawFrag.asInstanceOf[Fragment[Where.Concat[Slot, Void]]]
      case Some(f) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            rawFrag.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = TypedExpr.combineEnc[Slot, Void](f.encoder, rawFrag.encoder, proj)
        Fragment(parts, enc, Origin.unknown)
    }
  }

  /** Legacy alias — still used by some sites that haven't been updated. Falls through to product. */
  private[dsl] def combineEncoders(a: Encoder[?], b: Encoder[?]): Encoder[Any] = {
    val voidLeft  = a eq Void.codec
    val voidRight = b eq Void.codec
    if (voidLeft && voidRight) Void.codec.asInstanceOf[Encoder[Any]]
    else if (voidLeft)         b.asInstanceOf[Encoder[Any]]
    else if (voidRight)        a.asInstanceOf[Encoder[Any]]
    else                       a.asInstanceOf[Encoder[Any]].product(b.asInstanceOf[Encoder[Any]]).asInstanceOf[Encoder[Any]]
  }

  /**
   * Body-part. `Left(f)` is a `Fragment[Void]` whose encoder is already-baked (its argument flows via
   * contramap, typically a structural piece like " WHERE ", a header, or a row of values applied via
   * `Param.bind`). `Right(f)` is a typed `Fragment[A]` slot whose encoder takes a typed `A` at execute
   * time (typically a WHERE/HAVING predicate built from `Param[T]`).
   *
   * Splitting baked from typed at this level lets [[assembleN]] produce a final encoder that contramaps
   * the user-claimed `Args` correctly: the baked side is encoded with `Void` (its values flow via the
   * fragment's own contramap); the typed side receives the user's `Args` via `slotValues`.
   */
  private[dsl] type BodyPart = Either[Fragment[Void], Fragment[?]]

  /** Convert a pre-applied `AppliedFragment` into a `BodyPart` (Left). For static AFs whose encoder is
   * already `Void.codec`, [[TypedExpr.liftAfToVoid]] returns the underlying `Fragment[Void]` directly
   * with no allocation; for dynamic AFs it constructs a contramapped `Fragment[Void]` once. */
  private[dsl] inline def bake(af: AppliedFragment): BodyPart =
    Left(TypedExpr.liftAfToVoid(af))

  /** Build the body-parts list for a SELECT or projected SELECT in render order. */
  private[dsl] def bodyPartsAround(
    headerParts: List[AppliedFragment],
    whereOpt:    Option[Fragment[?]],
    groupBys:    List[TypedExpr[?, ?]],
    havingOpt:   Option[Fragment[?]],
    orderBys:    List[OrderBy[?]],
    limitOpt:    Option[Int],
    offsetOpt:   Option[Int],
    lockingOpt:  Option[Locking]
  ): List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart]()
    headerParts.foreach(af => buf += SelectBuilder.bake(af))
    whereOpt.foreach { f =>
      buf += SelectBuilder.bake(RawConstants.WHERE)
      buf += Right(f)
    }
    if (groupBys.nonEmpty) {
      buf += SelectBuilder.bake(RawConstants.GROUP_BY)
      buf += SelectBuilder.bake(TypedExpr.joined(groupBys.map(e => bindVoid(e.fragment)), ", "))
    }
    havingOpt.foreach { f =>
      buf += SelectBuilder.bake(RawConstants.HAVING)
      buf += Right(f)
    }
    if (orderBys.nonEmpty) {
      buf += SelectBuilder.bake(RawConstants.ORDER_BY)
      buf += SelectBuilder.bake(TypedExpr.joined(orderBys.map(o => bindVoid(o.fragment)), ", "))
    }
    limitOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += SelectBuilder.bake(TypedExpr.raw(" " + l.sql)))
    buf.toList
  }

  /**
   * Bind a `Fragment[?]` at `Void` to obtain an `AppliedFragment`. For groupBys / orderBys / DISTINCT ON
   * exprs that may carry typed Args (Param-bearing) — currently constrained to Void-args inputs (typed-args
   * threading through these positions is roadmap).
   */
  private[dsl] def bindVoid(f: Fragment[?]): AppliedFragment =
    f.asInstanceOf[Fragment[Void]].apply(Void)

  /**
   * Empty `Fragment[Void]` placeholder. Used as a Right slot filler when a typed position (WHERE /
   * GROUP BY / HAVING) is absent — keeps the assemble walker's slot index stable so subsequent
   * positions land on the correct A_i. Renders no SQL and emits no encoded value.
   */
  private[dsl] val emptyVoidSlot: Fragment[Void] = TypedExpr.voidFragment("")

  /**
   * Generic N-slot assembler. Each `Right(f)` in `bodyParts` is a typed slot; the i-th Right slot
   * gets its runtime value from `slotValues(args)(i)`. Callers build `slotValues` by unfolding the
   * nested `Where.Concat` chain via `Where.projectConcat` for their specific slot count.
   */
  private[dsl] def assembleN[Args, R](
    bodyParts:  List[BodyPart],
    ctes:       List[CteRelation[?, ?, ?, ?]],
    codec:      Codec[R],
    slotValues: Args => IArray[Any]
  ): QueryTemplate[Args, R] = {
    val ctePreambleParts: List[BodyPart] = renderWithPreambleParts(ctes)
    val allParts: List[BodyPart]         = ctePreambleParts ++ bodyParts
    val sqlParts: List[Either[String, cats.data.State[Int, String]]] =
      allParts.flatMap {
        case Left(f)  => f.parts
        case Right(f) => f.parts
      }
    // Fully-static fast path: if every part contributes no typed parameters (all encoders have empty
    // `types`), the runtime `encode` walk is guaranteed to return `Nil`. Skip the custom encoder and use
    // `Void.codec` directly — saves the per-execute parts iteration and matches the "static SQL" goal:
    // a `users.select.where(u => u.age >= lit(18)).compile` produces a `Fragment[Void]` whose encoder is
    // the trivial process-wide-shared `Void.codec`, so subsequent `.run(session)` calls bypass any
    // build-the-encoded-list work.
    val isFullyStatic: Boolean = allParts.forall {
      case Left(f)  => f.encoder.types.isEmpty
      case Right(f) => f.encoder.types.isEmpty
    }
    if (isFullyStatic) {
      val frag: Fragment[Args] = Fragment(sqlParts, Void.codec.asInstanceOf[Encoder[Args]], Origin.unknown)
      return QueryTemplate.mk[Args, R](frag, codec)
    }
    val finalEnc: Encoder[Args] = new Encoder[Args] {
      override val types: List[skunk.data.Type] =
        allParts.flatMap {
          case Left(f)  => f.encoder.types
          case Right(f) => f.encoder.types
        }
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          allParts.foldLeft((n0, "")) { case ((n, acc), part) =>
            val (n1, s) = part match {
              case Left(f)  => f.encoder.sql.run(n).value
              case Right(f) => f.encoder.sql.run(n).value
            }
            (n1, acc + s)
          }
        }
      override def encode(args: Args): List[Option[skunk.data.Encoded]] = {
        val slots = slotValues(args)
        var typedIdx = 0
        allParts.flatMap {
          case Left(f) =>
            f.encoder.asInstanceOf[Encoder[Void]].encode(Void)
          case Right(f) =>
            val enc = f.encoder.asInstanceOf[Encoder[Any]]
            val v   = slots(typedIdx)
            typedIdx += 1
            if (enc eq Void.codec) Nil else enc.encode(v)
        }
      }
    }
    val frag: Fragment[Args] = Fragment(sqlParts, finalEnc, Origin.unknown)
    QueryTemplate.mk[Args, R](frag, codec)
  }

}

/**
 * A SELECT with an explicit projection list — rows have shape `Row` instead of the relation's default named tuple.
 */
final class ProjectedSelect[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, DistinctOn <: Tuple, Orders <: Tuple, WArgs, HArgs, Row](
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean,
  private[sharp] val projections: List[TypedExpr[?, ?]],
  private[sharp] val codec: Codec[Row],
  private[sharp] val whereOpt: Option[Fragment[?]],
  private[sharp] val groupBys: List[TypedExpr[?, ?]],
  private[sharp] val havingOpt: Option[Fragment[?]],
  private[sharp] val orderBys: List[OrderBy[?]],
  private[sharp] val limitOpt: Option[Int],
  private[sharp] val offsetOpt: Option[Int],
  private[sharp] val lockingOpt: Option[Locking] = None,
  private[sharp] val distinctOnOpt: Option[List[TypedExpr[?, ?]]] = None
) {

  private def cp[W, H](
    distinct: Boolean = distinct,
    projections: List[TypedExpr[?, ?]] = projections,
    codec: Codec[Row] = codec,
    whereOpt: Option[Fragment[?]] = whereOpt,
    groupBys: List[TypedExpr[?, ?]] = groupBys,
    havingOpt: Option[Fragment[?]] = havingOpt,
    orderBys: List[OrderBy[?]] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt,
    distinctOnOpt: Option[List[TypedExpr[?, ?]]] = distinctOnOpt
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, W, H, Row] =
    new ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, W, H, Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )

  private def view: SelectView[Ss] = buildSelectView[Ss](sources)

  inline def where[A](f: SelectView[Ss] => Where[A]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, Where.Concat[WArgs, A], HArgs, Row] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred, c => Where.projectConcat[WArgs, A](c))
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  inline def whereRaw(af: AppliedFragment): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, ?, HArgs, Row] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af, c => Where.projectConcat[WArgs, Void](c))
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  /**
   * `ORDER BY …` — items are typed `OrderBy[A]` wrappers around `expr.asc / .desc / .nullsFirst /
   * .nullsLast`. `Orders` accumulates the wrapper types via `Tuple.Concat[Orders, NormProj[O]]` so
   * Param-bearing items thread their `A` into the QueryTemplate Args slot at `.compile` time
   * (similar to GROUP BY).
   */
  transparent inline def orderBy[O](inline f: SelectView[Ss] => O)
    : ProjectedSelect[Ss, Proj, Groups, DistinctOn, Tuple.Concat[Orders, NormProj[O]], WArgs, HArgs, Row] = {
    val v     = view
    val fresh = (f(v): Any) match {
      case ob: OrderBy[?] => List(ob)
      case t: Tuple       => t.toList.asInstanceOf[List[OrderBy[?]]]
    }
    new ProjectedSelect[Ss, Proj, Groups, DistinctOn, Tuple.Concat[Orders, NormProj[O]], WArgs, HArgs, Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys ++ fresh,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )
  }

  transparent inline def groupBy[G](inline f: SelectView[Ss] => G)
    : ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], DistinctOn, Orders, WArgs, HArgs, Row] = {
    val v     = view
    val fresh = (f(v): Any) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    new ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], DistinctOn, Orders, WArgs, HArgs, Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys ++ fresh,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )
  }

  inline def having[H](f: SelectView[Ss] => Where[H]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, Where.Concat[HArgs, H], Row] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[HArgs, H](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], pred, c => Where.projectConcat[HArgs, H](c))
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  inline def havingRaw(af: AppliedFragment): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, ?, Row] = {
    val combined = SelectBuilder.andRawInto[HArgs](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], af, c => Where.projectConcat[HArgs, Void](c))
    cp[WArgs, Any](havingOpt = Some(combined))
  }

  def limit(n: Int): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row]  = cp[WArgs, HArgs](limitOpt = Some(n))
  def offset(n: Int): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] = cp[WArgs, HArgs](offsetOpt = Some(n))

  def distinctRows: ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] = cp[WArgs, HArgs](distinct = true)

  /**
   * `DISTINCT ON (e1, e2, …)` projection. Items can be Param-bearing — at `.compile` time the
   * static `DistinctOn` type is folded by [[ProjArgsOf]] into a `DArgs` slot threaded ahead of the
   * projection list in render order.
   */
  transparent inline def distinctOn[D](inline f: SelectView[Ss] => D)
    : ProjectedSelect[Ss, Proj, Groups, NormProj[D], Orders, WArgs, HArgs, Row] = {
    val v     = view
    val exprs = (f(v): Any) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    new ProjectedSelect[Ss, Proj, Groups, NormProj[D], Orders, WArgs, HArgs, Row](
      sources,
      distinct,
      projections,
      codec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      Some(exprs)
    )
  }

  def forUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = Row & Tuple }
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, T] = {
    val newCodec: Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[Row]
    )
    new ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, HArgs, T](
      sources,
      distinct,
      projections,
      newCodec,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt,
      distinctOnOpt
    )
  }

  /**
   * Compile into a [[QueryTemplate]]. Enforces [[GroupCoverage]] and threads typed `Args` from
   * Param-bearing items in **seven** logical positions in render order:
   *
   *   - `DArgs`    — `DISTINCT ON` items (via [[ProjArgsOf]] over `DistinctOn`).
   *   - `ProjArgs` — projection items (via [[ProjArgsOf]] over `Proj`).
   *   - `SArgs`    — source body args (inner query Args when source is a typed subquery via `.alias`).
   *   - `WArgs`    — WHERE clause.
   *   - `GArgs`    — GROUP BY items (via [[ProjArgsOf]] over `Groups`).
   *   - `HArgs`    — HAVING clause.
   *   - `OArgs`    — ORDER BY items (via [[ProjArgsOf]] over `Orders`).
   *
   * For plain table / view sources `SArgs = Void` and the result type is identical to the prior
   * 6-slot form: `Concat[Concat[Concat[Concat[Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs], OArgs]`.
   */
  // Concat-chain evidences. Each name spells the accumulator at that step (left-fold over the slot order):
  // CArgs ⊕ DArgs ⊕ ProjArgs ⊕ SArgs ⊕ OnA ⊕ WArgs ⊕ GArgs ⊕ HArgs ⊕ OArgs.
  //   cd   = CArgs ⊕ DArgs                                          → CD
  //   cdp  = CD ⊕ ProjArgs                                          → CDP
  //   cdps = CDP ⊕ SArgs                                            → CDPS
  //   cdpso  = CDPS ⊕ OnA                                           → CDPSO
  //   cdpsow = CDPSO ⊕ WArgs                                        → CDPSOW
  //   cdpsowg = CDPSOW ⊕ GArgs                                      → CDPSOWG
  //   cdpsowgh = CDPSOWG ⊕ HArgs                                    → CDPSOWGH
  //   cdpsowgho = CDPSOWGH ⊕ OArgs                                  → outer Args
  inline def compile[SArgs, OnA, CArgs, DArgs, ProjArgs, GArgs, OArgs](using
    ev:      GroupCoverage[Proj, Groups],
    sbOf:    SourceBodyArgsOf.Aux[Ss, SArgs],
    bff:     SourceBodyArgsProj[Ss],
    onSum:   SourceOnArgsOf.Aux[Ss, OnA],
    onProj:  SourceOnArgsProj[Ss],
    cteSum:  CteArgsOf.Aux[Ss, CArgs],
    cteProj: CteArgsProj[Ss],
    d:       ProjArgsOf.Aux[DistinctOn, DArgs],
    pa:      ProjArgsOf.Aux[Proj, ProjArgs],
    g:       ProjArgsOf.Aux[Groups, GArgs],
    o:       ProjArgsOf.Aux[Orders, OArgs]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs], GArgs], HArgs], OArgs], Row] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
    val ctes    = collectCtesInOrder(entries)
    val rawDistProjector  = d.project.asInstanceOf[Any => List[Any]]
    val rawProjProjector  = pa.project.asInstanceOf[Any => List[Any]]
    val rawGroupProjector = g.project.asInstanceOf[Any => List[Any]]
    val rawOrderProjector = o.project.asInstanceOf[Any => List[Any]]
    val distinctSize      = distinctOnOpt.fold(0)(_.size)
    val distProjector: Any => List[Any] = a => { val xs = rawDistProjector(a);  if (xs.size == distinctSize)     xs else List.fill(distinctSize)(Void) }
    val projProjector: Any => List[Any] = a => { val xs = rawProjProjector(a);  if (xs.size == projections.size) xs else List.fill(projections.size)(Void) }
    val groupProjector: Any => List[Any] = a => { val xs = rawGroupProjector(a); if (xs.size == groupBys.size)    xs else List.fill(groupBys.size)(Void) }
    val orderProjector: Any => List[Any] = a => { val xs = rawOrderProjector(a); if (xs.size == orderBys.size)    xs else List.fill(orderBys.size)(Void) }
    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs], GArgs], HArgs], OArgs]
    val srcSlotCount = sourceSlotCount(entries)
    val slotValues: Out => IArray[Any] = args => {
      val (cdpsowghAcc, oArgs) = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs], GArgs], HArgs], OArgs](args)
      val (cdpsowgAcc, hArgs)  = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs], GArgs], HArgs](cdpsowghAcc)
      val (cdpsowAcc, gArgs)   = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs], GArgs](cdpsowgAcc)
      val (cdpsoAcc, wArgs)    = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA], WArgs](cdpsowAcc)
      val (cdpsAcc, onArgs)    = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs], OnA](cdpsoAcc)
      val (cdpAcc, sArgs)      = Where.projectConcat[Where.Concat[Where.Concat[CArgs, DArgs], ProjArgs], SArgs](cdpsAcc)
      val (cdAcc, pArgs)       = Where.projectConcat[Where.Concat[CArgs, DArgs], ProjArgs](cdpAcc)
      val (cArgs, dArgs)       = Where.projectConcat[CArgs, DArgs](cdAcc)
      val baseSlots = buildSlotIArray(dArgs, pArgs, sArgs, onArgs, srcSlotCount, bff, onProj, wArgs, gArgs, hArgs, oArgs)
      buildCteAndSlotIArrayWithEntries(entries, cteProj, cArgs, ctes, baseSlots)
    }
    SelectBuilder.assembleN[Out, Row](
      bodyParts  = compileBodyParts(distProjector, projProjector, groupProjector, orderProjector),
      ctes       = ctes,
      codec      = codec,
      slotValues = slotValues
    )
  }

  /**
   * Produces a typed `Fragment[CombinedArgs]` for the SELECT body **without** any CTE preamble.
   * Used by [[ProjectedSelect.alias]] to capture the inner query fragment.
   *
   * Slot order: `[DIST=0, PROJ=1, SRC=2, WHERE=3, GROUP=4, HAVING=5, ORDER=6]`.
   */
  // Concat-chain evidences. Each name spells the accumulator at that step (left-fold over the slot order):
  // DArgs ⊕ ProjArgs ⊕ SArgs ⊕ OnA ⊕ WArgs ⊕ GArgs ⊕ HArgs ⊕ OArgs. (No CArgs here — `compileBodyFragment`
  // captures the body alone; CTE refs in this body bind at the OUTER query's WITH preamble.)
  private[dsl] inline def compileBodyFragment[SA, OnA, DA2, PA, GA, OA2](using
    ev:      GroupCoverage[Proj, Groups],
    sbOf:    SourceBodyArgsOf.Aux[Ss, SA],
    bff:     SourceBodyArgsProj[Ss],
    onSum:   SourceOnArgsOf.Aux[Ss, OnA],
    onProj:  SourceOnArgsProj[Ss],
    d:       ProjArgsOf.Aux[DistinctOn, DA2],
    pa:      ProjArgsOf.Aux[Proj, PA],
    g:       ProjArgsOf.Aux[Groups, GA],
    o:       ProjArgsOf.Aux[Orders, OA2]
  ): Fragment[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs], GA], HArgs], OA2]] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
    val rawDistProjector:  Any => List[Any] = d.project.asInstanceOf[Any => List[Any]]
    val rawProjProjector:  Any => List[Any] = pa.project.asInstanceOf[Any => List[Any]]
    val rawGroupProjector: Any => List[Any] = g.project.asInstanceOf[Any => List[Any]]
    val rawOrderProjector: Any => List[Any] = o.project.asInstanceOf[Any => List[Any]]
    val distinctSize = distinctOnOpt.fold(0)(_.size)
    val distProjector:  Any => List[Any] = a => { val xs = rawDistProjector(a);  if (xs.size == distinctSize)     xs else List.fill(distinctSize)(Void) }
    val projProjector:  Any => List[Any] = a => { val xs = rawProjProjector(a);  if (xs.size == projections.size) xs else List.fill(projections.size)(Void) }
    val groupProjector: Any => List[Any] = a => { val xs = rawGroupProjector(a); if (xs.size == groupBys.size)    xs else List.fill(groupBys.size)(Void) }
    val orderProjector: Any => List[Any] = a => { val xs = rawOrderProjector(a); if (xs.size == orderBys.size)    xs else List.fill(orderBys.size)(Void) }
    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs], GA], HArgs], OA2]
    val srcSlotCount = sourceSlotCount(entries)
    val slotValues: Out => IArray[Any] = args => {
      val (dpsowghAcc, oArgs) = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs], GA], HArgs], OA2](args)
      val (dpsowgAcc, hArgs)  = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs], GA], HArgs](dpsowghAcc)
      val (dpsowAcc, gArgs)   = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs], GA](dpsowgAcc)
      val (dpsoAcc, wArgs)    = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WArgs](dpsowAcc)
      val (dpsAcc, onArgs)    = Where.projectConcat[Where.Concat[Where.Concat[DA2, PA], SA], OnA](dpsoAcc)
      val (dpAcc, sArgs)      = Where.projectConcat[Where.Concat[DA2, PA], SA](dpsAcc)
      val (dArgs, pArgs)      = Where.projectConcat[DA2, PA](dpAcc)
      buildSlotIArray(dArgs, pArgs, sArgs, onArgs, srcSlotCount, bff, onProj, wArgs, gArgs, hArgs, oArgs)
    }
    SelectBuilder.assembleN[Out, Row](
      bodyParts  = compileBodyParts(distProjector, projProjector, groupProjector, orderProjector),
      ctes       = Nil,
      codec      = codec,
      slotValues = slotValues
    ).fragment
  }

  /**
   * Build body parts with stable slot indices:
   * `[DIST=0, PROJ=1, SRC=2, WHERE=3, GROUP=4, HAVING=5, ORDER=6]`.
   *
   * Slot 2 (SRC) is the source-body slot: typed subquery → `Right(innerFrag)`; plain source →
   * `Right(emptyVoidSlot)`.  All slots are always emitted (absent clauses use `emptyVoidSlot`).
   */
  private def compileBodyParts(
    distProjector:  Any => List[Any],
    projProjector:  Any => List[Any],
    groupProjector: Any => List[Any],
    orderProjector: Any => List[Any]
  ): List[SelectBuilder.BodyPart] = {
    val entries      = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
    val combinedProj = TypedExpr.combineList[Any](projections.map(_.fragment), ", ", projProjector)
    val buf          = scala.collection.mutable.ListBuffer[BodyPart]()
    // slot 0 = DISTINCT ON
    distinctOnOpt match {
      case Some(exprs) =>
        val combinedDist = TypedExpr.combineList[Any](exprs.map(_.fragment), ", ", distProjector)
        buf += SelectBuilder.bake(RawConstants.SELECT_DISTINCT_ON)
        buf += Right(combinedDist)
        buf += SelectBuilder.bake(RawConstants.CLOSE_PAREN_SPACE)
      case None =>
        buf += SelectBuilder.bake(if (distinct) RawConstants.SELECT_DISTINCT else RawConstants.SELECT)
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    buf += Right(combinedProj) // slot 1 = PROJ
    // Per source, two Right slots in render order: body, ON. Head and CROSS sources contribute
    // `emptyVoidSlot` for ON (no predicate). Typed subqueries contribute their inner `Fragment[BodyArgs]` for
    // body; typed ON predicates contribute their fragment with a baked " ON " prefix. SourceBodyArgsProj +
    // SourceOnArgsProj split the combined `SArgs` / `OnArgs` back into per-source values for the slotValues
    // IArray that `assembleN` consumes.
    if (entries.nonEmpty && entries.head.relation.hasFromClause) {
      val head = entries.head
      buf += SelectBuilder.bake(RawConstants.FROM)
      aliasedFromEntryParts(head).foreach(buf += _)
      buf += Right(SelectBuilder.emptyVoidSlot) // head has no ON
      entries.tail.foreach { s =>
        buf += SelectBuilder.bake(if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
        aliasedFromEntryParts(s).foreach(buf += _)
        s.onPredOpt match {
          case Some(p) =>
            buf += Right(SelectBuilder.prefixedFrag(RawConstants.ON, p.fragment.asInstanceOf[Fragment[Any]]))
          case None    =>
            buf += Right(SelectBuilder.emptyVoidSlot)
        }
      }
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot) // single Void body slot when there's no FROM
      buf += Right(SelectBuilder.emptyVoidSlot) // matching ON slot
    }
    // slot 3 = WHERE
    whereOpt match {
      case Some(f) =>
        buf += SelectBuilder.bake(RawConstants.WHERE)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 4 = GROUP BY
    if (groupBys.nonEmpty) {
      val combinedGrp = TypedExpr.combineList[Any](groupBys.map(_.fragment), ", ", groupProjector)
      buf += SelectBuilder.bake(RawConstants.GROUP_BY)
      buf += Right(combinedGrp)
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 5 = HAVING
    havingOpt match {
      case Some(f) =>
        buf += SelectBuilder.bake(RawConstants.HAVING)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 6 = ORDER BY
    if (orderBys.nonEmpty) {
      val combinedOrd = TypedExpr.combineList[Any](orderBys.map(_.fragment), ", ", orderProjector)
      buf += SelectBuilder.bake(RawConstants.ORDER_BY)
      buf += Right(combinedOrd)
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot)
    }
    limitOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += SelectBuilder.bake(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += SelectBuilder.bake(TypedExpr.raw(" " + l.sql)))
    buf.toList
  }

}

/**
 * Number of (body, on) source-slot PAIRS emitted by [[ProjectedSelect.compileBodyParts]] for a given source
 * list. Each source contributes two slots — a body slot and an ON slot. Head and CROSS sources contribute
 * `emptyVoidSlot` for their ON. The FROM-less branch emits one body + one ON placeholder pair regardless.
 */
private[dsl] def sourceSlotCount(entries: List[SourceEntry[?, ?, ?, ?, ?]]): Int =
  if (entries.nonEmpty && entries.head.relation.hasFromClause) entries.size else 1

/**
 * Map of CTE name → body-args value, built from a `CteArgsProj` projection over the source-tuple combined
 * `cArgs` value. Plain (non-CteRelation) source positions yield Void and are filtered out.
 *
 * Walks `entries` in source order to extract CTE names from `IsCte` relations, zipping with the projected
 * per-source args list. The result is consumed by [[buildCtePreambleSlots]] which emits one Void or typed
 * value per collected CTE in dep order.
 */
private[dsl] def cteDirectArgsByName(
  entries:  List[SourceEntry[?, ?, ?, ?, ?]],
  cteProj:  CteArgsProj[? <: Tuple],
  cArgs:    Any
): Map[String, Any] = {
  val perSource = cteProj.project(cArgs)
  if (perSource.isEmpty) Map.empty
  else entries.zip(perSource).collect {
    case (e, v) if e.relation.isInstanceOf[IsCte] =>
      e.relation.asInstanceOf[IsCte].underlyingCte.cteName -> v
  }.toMap
}

/**
 * Per-CTE body-args slot values in collected (dep) order. CTEs that appear as direct refs in `entries` look
 * up their args in the projected `cArgs`; transitive deps (constrained `Void` by [[CteDepsAllVoid]]) get
 * `Void`.
 */
private[dsl] def buildCtePreambleSlots(
  ctes:           List[CteRelation[?, ?, ?, ?]],
  directArgsByName: Map[String, Any]
): IArray[Any] =
  IArray.from(ctes.map(c => directArgsByName.getOrElse(c.cteName, Void)))

/**
 * Prepend per-CTE preamble slot values to a base IArray of body slot values. Walks `entries` in source
 * order to map projected `cArgs` values to CTE names, then assembles preamble slot values in dep order
 * (matching `renderWithPreambleParts`'s emission order). Transitive-dep CTEs (constrained Void by
 * [[CteDepsAllVoid]]) get Void slots.
 */
private[dsl] def buildCteAndSlotIArrayWithEntries(
  entries:    List[SourceEntry[?, ?, ?, ?, ?]],
  cteProj:    CteArgsProj[? <: Tuple],
  cArgs:      Any,
  collected:  List[CteRelation[?, ?, ?, ?]],
  baseSlots:  IArray[Any]
): IArray[Any] =
  if (collected.isEmpty) baseSlots
  else {
    val directArgsByName = cteDirectArgsByName(entries, cteProj, cArgs)
    val cteSlotValues    = buildCtePreambleSlots(collected, directArgsByName)
    val out              = new Array[Any](cteSlotValues.length + baseSlots.length)
    cteSlotValues.copyToArray(out, 0)
    baseSlots.copyToArray(out, cteSlotValues.length)
    IArray.unsafeFromArray(out)
  }

/**
 * Assemble the `IArray[Any]` slot values for a [[ProjectedSelect.compile]] / `compileBodyFragment` body.
 * Layout matches the body parts emitted by `compileBodyParts`:
 * `[DIST, PROJ, body_1, on_1, body_2, on_2, …, body_N, on_N, WHERE, GROUP, HAVING, ORDER]` — body and ON
 * slots are interleaved, in source order, two per source.
 *
 * Per-source body args come from `SourceBodyArgsProj`; per-source ON args come from `SourceOnArgsProj`.
 * Both produce N-element lists in source order; we zip them positionally. When the head is FROM-less
 * (single placeholder pair) we replace the projected lists with `Void` placeholders.
 */
private[dsl] def buildSlotIArray(
  dArgs:        Any,
  pArgs:        Any,
  sArgs:        Any,
  onArgs:       Any,
  srcSlotCount: Int,
  bff:          SourceBodyArgsProj[? <: Tuple],
  onProj:       SourceOnArgsProj[? <: Tuple],
  wArgs:        Any,
  gArgs:        Any,
  hArgs:        Any,
  oArgs:        Any
): IArray[Any] = {
  val perBody: List[Any] = {
    val projected = bff.project(sArgs)
    if (srcSlotCount == 1 && projected.size != 1) List(Void) else projected
  }
  val perOn: List[Any] = {
    val projected = onProj.project(onArgs)
    if (srcSlotCount == 1 && projected.size != 1) List(Void) else projected
  }
  val out = scala.collection.mutable.ArrayBuffer.empty[Any]
  out += dArgs
  out += pArgs
  perBody.zipAll(perOn, Void, Void).foreach { case (b, o) => out += b; out += o }
  out += wArgs
  out += gArgs
  out += hArgs
  out += oArgs
  IArray.from(out)
}

/** Render the `SELECT`-keyword prefix with trailing space. */
private[dsl] def renderSelectPrefix(
  distinct: Boolean,
  distinctOnOpt: Option[List[TypedExpr[?, ?]]]
): skunk.AppliedFragment = {
  import skunk.sharp.internal.RawConstants.*
  distinctOnOpt match {
    case Some(exprs) =>
      // distinctOn exprs may carry typed Args; bind them at Void here for the AF prefix path.
      // Typed-args threading through DISTINCT ON is roadmap.
      val joined = exprs.map(e => e.fragment.asInstanceOf[Fragment[Void]].apply(Void))
      SELECT_DISTINCT_ON |+| TypedExpr.joined(joined, ", ") |+| CLOSE_PAREN_SPACE
    case None =>
      if (distinct) SELECT_DISTINCT
      else SELECT
  }
}

// ---- Entry points -----------------------------------------------------------------------------

extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton, ML <: AliasMode](left: L)(using
  aL: AsRelation.Aux[L, RL, CL, AL, ML]
) {

  def select: SelectBuilder[SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple, EmptyTuple, Void, Void] = {
    val entry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    new SelectBuilder[SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple, EmptyTuple, Void, Void](entry *: EmptyTuple)
  }

}

/** `empty.select(…)` — FROM-less SELECT. */
extension (rel: skunk.sharp.empty.type) {

  def select[T, A](e: TypedExpr[T, A]): ProjectedSelect[EmptyTuple, TypedExpr[T, A] *: EmptyTuple, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, T] =
    new ProjectedSelect[EmptyTuple, TypedExpr[T, A] *: EmptyTuple, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, T](
      EmptyTuple,
      false,
      List(e),
      e.codec,
      None,
      Nil,
      None,
      Nil,
      None,
      None,
      None
    )

  def select[X <: NonEmptyTuple](t: X): ProjectedSelect[EmptyTuple, X, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, ExprOutputs[X]] = {
    val exprs = t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[X]]]
    new ProjectedSelect[EmptyTuple, X, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, ExprOutputs[X]](
      EmptyTuple,
      false,
      exprs,
      codec,
      None,
      Nil,
      None,
      Nil,
      None,
      None,
      None
    )
  }

  /** Same erasedValue dispatch as the table-bound `select`; sources is `EmptyTuple` (FROM-less). */
  transparent inline def select[X](inline f: ColumnsView[EmptyTuple] => X) = {
    val v = ColumnsView(EmptyTuple)
    inline scala.compiletime.erasedValue[X] match {
      case _: TypedExpr[?, ?] =>
        val expr = f(v).asInstanceOf[TypedExpr[?, ?]]
        new ProjectedSelect[EmptyTuple, X *: EmptyTuple, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, ProjResult[X]](
          EmptyTuple,
          false,
          List(expr),
          expr.codec.asInstanceOf[Codec[ProjResult[X]]],
          None,
          Nil,
          None,
          Nil,
          None,
          None,
          None
        )
      case _: scala.NamedTuple.AnyNamedTuple =>
        val tup   = f(v).asInstanceOf[Product]
        val exprs = tup.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
        val codec = tupleCodec(exprs.map(_.codec))
          .asInstanceOf[Codec[scala.NamedTuple.NamedTuple[
            scala.NamedTuple.Names[X & scala.NamedTuple.AnyNamedTuple],
            ExprOutputs[scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple]]
          ]]]
        new ProjectedSelect[
          EmptyTuple,
          scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple],
          EmptyTuple,
          EmptyTuple,
          EmptyTuple,
          Void,
          Void,
          scala.NamedTuple.NamedTuple[
            scala.NamedTuple.Names[X & scala.NamedTuple.AnyNamedTuple],
            ExprOutputs[scala.NamedTuple.DropNames[X & scala.NamedTuple.AnyNamedTuple]]
          ]
        ](
          EmptyTuple,
          false,
          exprs,
          codec,
          None,
          Nil,
          None,
          Nil,
          None,
          None,
          None
        )
      case _: NonEmptyTuple =>
        val tup   = f(v).asInstanceOf[NonEmptyTuple]
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?, ?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[X & Tuple]]]
        new ProjectedSelect[EmptyTuple, X & Tuple, EmptyTuple, EmptyTuple, EmptyTuple, Void, Void, ExprOutputs[X & Tuple]](
          EmptyTuple,
          false,
          exprs,
          codec,
          None,
          Nil,
          None,
          Nil,
          None,
          None,
          None
        )
    }
  }

}

// ---- Match type: view receiver for lambdas -----------------------------------------------------

type SelectView[Ss <: Tuple] = Ss match {
  case SourceEntry[?, ?, c, ?, ?] *: EmptyTuple => ColumnsView[c]
  case _                                     => JoinedView[Ss]
}

private[sharp] def buildSelectView[Ss <: Tuple](sources: Ss): SelectView[Ss] =
  sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]] match {
    case single :: Nil =>
      if (single.alias == single.relation.name) {
        if (single.effectiveCols eq single.relation.columns)
          single.relation.columnsView.asInstanceOf[SelectView[Ss]]
        else
          ColumnsView(single.effectiveCols).asInstanceOf[SelectView[Ss]]
      } else
        ColumnsView.qualified(single.effectiveCols, single.alias).asInstanceOf[SelectView[Ss]]
    case _ => buildJoinedView[Ss](sources).asInstanceOf[SelectView[Ss]]
  }

// ---- Evidence typeclasses ----------------------------------------------------------------------

sealed trait IsSingleTable[Ss]

object IsSingleTable {

  given [Cols <: Tuple, N <: String & Singleton, A <: String & Singleton]
    : IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A, Void] *: EmptyTuple] =
    new IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A, Void] *: EmptyTuple] {}

}

sealed trait IsSingleSource[Ss] {
  type Cols <: Tuple
}

object IsSingleSource {
  type Aux[Ss, C] = IsSingleSource[Ss] { type Cols = C }

  given [RR <: Relation[C], C <: Tuple, A <: String & Singleton]
    : IsSingleSource.Aux[SourceEntry[RR, C, C, A, Void] *: EmptyTuple, C] =
    new IsSingleSource[SourceEntry[RR, C, C, A, Void] *: EmptyTuple] { type Cols = C }

}

// ---- Locking enums + OrderBy + projection helpers ---------------------------------------------

enum LockMode(val sql: String) {
  case ForUpdate      extends LockMode("FOR UPDATE")
  case ForNoKeyUpdate extends LockMode("FOR NO KEY UPDATE")
  case ForShare       extends LockMode("FOR SHARE")
  case ForKeyShare    extends LockMode("FOR KEY SHARE")
}

enum WaitPolicy(val sql: String) {
  case Wait       extends WaitPolicy("")
  case NoWait     extends WaitPolicy(" NOWAIT")
  case SkipLocked extends WaitPolicy(" SKIP LOCKED")
}

final case class Locking(mode: LockMode, waitPolicy: WaitPolicy = WaitPolicy.Wait) {
  def sql: String = mode.sql + waitPolicy.sql
}

type ExprOutputs[T <: Tuple] <: Tuple = T match {
  case EmptyTuple              => EmptyTuple
  case TypedExpr[t, ?] *: tail => t *: ExprOutputs[tail]
}

/**
 * Dual of [[ExprOutputs]] — extracts the per-item `Args` slot from a tuple of `TypedExpr`s. A column
 * reference contributes `Void`; a `Param[T]` contributes `T`. Combined with [[Where.FoldConcat]] this
 * gives the result Args for variadic / multi-item DSL positions.
 */
type CollectArgs[T <: Tuple] <: Tuple = T match {
  case EmptyTuple              => EmptyTuple
  case TypedExpr[?, a] *: tail => a *: CollectArgs[tail]
}

type ProjResult[X] = X match {
  case TypedExpr[t, ?] => t
  case NonEmptyTuple   => ExprOutputs[X & NonEmptyTuple]
}

type NormProj[X] <: Tuple = X match {
  case NonEmptyTuple => X & Tuple
  case _             => X *: EmptyTuple
}

type LookupTypes[Cols <: Tuple, Names <: Tuple] <: Tuple = Names match {
  case EmptyTuple => EmptyTuple
  case n *: rest  => ColumnType[Cols, n & String & Singleton] *: LookupTypes[Cols, rest]
}

/**
 * ORDER BY entry — typed `Fragment[A]` parametrised over the underlying TypedExpr's `Args` so
 * Param-bearing exprs (`Param[Int].desc`) thread `A` into the assembled query.
 */
final case class OrderBy[A](fragment: Fragment[A]) {
  def nullsFirst: OrderBy[A] = OrderBy(appendKw(fragment, " NULLS FIRST"))
  def nullsLast: OrderBy[A]  = OrderBy(appendKw(fragment, " NULLS LAST"))
  private def appendKw(f: Fragment[A], s: String): Fragment[A] = {
    val parts = f.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s))
    Fragment(parts, f.encoder, Origin.unknown)
  }
}

extension [T, A](expr: TypedExpr[T, A]) {
  def asc: OrderBy[A]  = OrderBy(appendKw(expr.fragment, " ASC"))
  def desc: OrderBy[A] = OrderBy(appendKw(expr.fragment, " DESC"))
  private def appendKw(f: Fragment[A], s: String): Fragment[A] = {
    val parts = f.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s))
    Fragment(parts, f.encoder, Origin.unknown)
  }
}
