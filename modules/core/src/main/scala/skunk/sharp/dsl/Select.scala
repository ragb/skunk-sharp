package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, RawConstants}
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
final class SelectBuilder[Ss <: Tuple, WArgs, HArgs] private[sharp] (
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
  ): SelectBuilder[Ss, W, H] =
    new SelectBuilder[Ss, W, H](
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
  def where[A](f: SelectView[Ss] => Where[A])(using
    c2: Where.Concat2[WArgs, A]
  ): SelectBuilder[Ss, Where.Concat[WArgs, A], HArgs] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred)
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  /** Escape hatch — widens `WArgs` to `?`. */
  def whereRaw(af: AppliedFragment)(using
    c2: Where.Concat2[WArgs, Void]
  ): SelectBuilder[Ss, ?, HArgs] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af)
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  /** `ORDER BY …` — typed exprs may carry their own Args; absorbed into the assembled fragment encoder. */
  def orderBy(f: SelectView[Ss] => OrderBy[?] | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val fresh = f(view) match {
      case ob: OrderBy[?] => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy[?]]]
    }
    cp[WArgs, HArgs](orderBys = orderBys ++ fresh)
  }

  /** `GROUP BY …` on a pre-projection builder. */
  def groupBy(f: SelectView[Ss] => TypedExpr[?, ?] | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val fresh = f(view) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    cp[WArgs, HArgs](groupBys = groupBys ++ fresh)
  }

  /** `HAVING <typed-predicate>`. */
  def having[H](f: SelectView[Ss] => Where[H])(using
    c2: Where.Concat2[HArgs, H]
  ): SelectBuilder[Ss, WArgs, Where.Concat[HArgs, H]] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[HArgs, H](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], pred)
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  /** Escape hatch HAVING — widens `HArgs` to `?`. */
  def havingRaw(af: AppliedFragment)(using
    c2: Where.Concat2[HArgs, Void]
  ): SelectBuilder[Ss, WArgs, ?] = {
    val combined = SelectBuilder.andRawInto[HArgs](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], af)
    cp[WArgs, Any](havingOpt = Some(combined))
  }

  def limit(n: Int): SelectBuilder[Ss, WArgs, HArgs]  = cp[WArgs, HArgs](limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[Ss, WArgs, HArgs] = cp[WArgs, HArgs](offsetOpt = Some(n))

  def distinctRows: SelectBuilder[Ss, WArgs, HArgs] = cp[WArgs, HArgs](distinct = true)

  /** `SELECT DISTINCT ON (e1, e2, …) …`. */
  def distinctOn(f: SelectView[Ss] => TypedExpr[?, ?] | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val exprs = f(view) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    }
    cp[WArgs, HArgs](distinctOnOpt = Some(exprs))
  }

  // ---- Row-level locking (single-source Table only) ---------------------------------------------

  def forUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): SelectBuilder[Ss, WArgs, HArgs] =
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
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]], WArgs, HArgs] = {
    val rel   = a(next)
    val cols  = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](rel, a.aliasValue(next), cols, cols, JoinKind.Cross, None)
    val next2 = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]], WArgs, HArgs](
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
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]], WArgs, HArgs] = {
    val t     = fn(view)
    val rel   = a(t)
    val cols  = rel.columns.asInstanceOf[CR]
    val entry = new SourceEntry[RR, CR, CR, AR](
      rel,
      a.aliasValue(t),
      cols,
      cols,
      JoinKind.Cross,
      None,
      isLateral = true
    )
    val next2 = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR, CR, AR]], WArgs, HArgs](
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
        new ProjectedSelect[Ss, X *: EmptyTuple, EmptyTuple, EmptyTuple, EmptyTuple, WArgs, HArgs, ProjResult[X]](
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
          EmptyTuple,
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
        new ProjectedSelect[Ss, X & Tuple, EmptyTuple, EmptyTuple, EmptyTuple, WArgs, HArgs, ExprOutputs[X & Tuple]](
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
   * Bridge for [[Cte]] / `SelectBuilder.alias` that still need an `AppliedFragment`. Renders the SELECT body
   * **without** the CTE preamble (the outer query's `assemble` is responsible for emitting `WITH` once).
   * Currently constrained to `WArgs = Void` and `HArgs = Void` — typed-args threading through CTE bodies and
   * aliased subqueries is roadmap.
   */
  private[dsl] def compileFragment(using ev: IsSingleSource[Ss]): AppliedFragment = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head
    val tpl     = SelectBuilder.assemble[Void, Void, NamedRowOf[ev.Cols]](
      bodyParts = compileBodyParts(head),
      ctes      = Nil, // outer query owns the WITH preamble
      codec     = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]
    )
    tpl.fragment.asInstanceOf[Fragment[Void]].apply(Void)
  }

  /**
   * Whole-row `.compile` — only on single-source builders. Returns `QueryTemplate[Concat[WArgs, HArgs], Row]`.
   */
  def compile(using
    ev: IsSingleSource[Ss],
    c2: Where.Concat2[WArgs, HArgs]
  ): QueryTemplate[Where.Concat[WArgs, HArgs], NamedRowOf[ev.Cols]] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head
    val ctes    = collectCtesInOrder(entries)
    SelectBuilder.assemble[WArgs, HArgs, NamedRowOf[ev.Cols]](
      bodyParts = compileBodyParts(head),
      ctes      = ctes,
      codec     = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]
    )(using c2)
  }

  private def compileBodyParts(head: SourceEntry[?, ?, ?, ?]): List[SelectBuilder.BodyPart] = {
    val rel          = head.relation
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val headerParts  = scala.collection.mutable.ListBuffer[AppliedFragment](selectPrefix)
    val canCacheCols = (head.effectiveCols eq rel.columns) && (head.alias == rel.currentAlias)
    if (rel.hasFromClause) {
      if (canCacheCols) {
        rel.starProjFromAfOpt match {
          case Some(af) => headerParts += af
          case None =>
            headerParts += rel.starProjAf
            headerParts += RawConstants.FROM
            headerParts += aliasedFromEntry(head)
        }
      } else {
        val cols    = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
        val projStr = cols.map(c => s""""${c.name}"""").mkString(", ")
        headerParts += TypedExpr.raw(s"$projStr FROM ")
        headerParts += aliasedFromEntry(head)
      }
    } else {
      if (canCacheCols) {
        headerParts += rel.starProjAf
      } else {
        val cols    = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
        val projStr = cols.map(c => s""""${c.name}"""").mkString(", ")
        headerParts += TypedExpr.raw(projStr)
      }
    }
    val tail = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    tail.foreach { s =>
      headerParts += (if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
      headerParts += aliasedFromEntry(s)
      s.onPredOpt.foreach { p =>
        headerParts += RawConstants.ON
        // ON predicates are typed expressions; bind their Args at Void here to lift to AppliedFragment.
        // Typed-args threading through ON predicates is roadmap.
        headerParts += SelectBuilder.bindVoid(p.fragment)
      }
    }
    SelectBuilder.bodyPartsAround(
      headerParts.toList,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

}

object SelectBuilder {

  // ---- AND-into helpers (typed and raw) --------------------------------------------------------

  /**
   * AND a typed predicate into an existing typed slot. Uses [[Where.Concat2]] so the combined encoder's input
   * shape matches `Concat[Slot, A]` rather than the raw Tuple2 the underlying product expects.
   */
  private[dsl] def andInto[Slot, A](
    slot: Option[Fragment[Slot]], pred: Where[A]
  )(using c2: Where.Concat2[Slot, A]): Fragment[Where.Concat[Slot, A]] =
    slot match {
      case None    => pred.fragment.asInstanceOf[Fragment[Where.Concat[Slot, A]]]
      case Some(f) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            pred.fragment.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = TypedExpr.combineEnc[Slot, A](f.encoder, pred.fragment.encoder)
        Fragment(parts, enc, Origin.unknown)
    }

  /** AND a pre-applied raw `AppliedFragment` into a slot — bakes its args via contramap (treats raw as Void-args). */
  private[dsl] def andRawInto[Slot](
    slot: Option[Fragment[Slot]], af: AppliedFragment
  )(using c2: Where.Concat2[Slot, Void]): Fragment[Where.Concat[Slot, Void]] = {
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
        val enc = TypedExpr.combineEnc[Slot, Void](f.encoder, rawFrag.encoder)
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
   * Body-part. `Left(af)` is a pre-applied `AppliedFragment` (its args are already baked — typically a
   * structural piece like " WHERE ", a header, or a row of values applied via `Param.bind`). `Right(f)` is a
   * typed `Fragment[A]` slot whose encoder takes a typed `A` at execute time (typically a WHERE/HAVING
   * predicate built from `Param[T]`).
   *
   * Splitting baked from typed at this level lets [[assemble]] produce a final encoder that contramaps the
   * user-claimed `Args` correctly: the baked side is contramapped to discard the user's input and emit its
   * pre-known values; the typed side receives the user's `Args`. Products inside the same side stay
   * type-aligned (typed-typed products yield nested tuples that match `Where.Concat` reductions).
   */
  private[dsl] type BodyPart = Either[AppliedFragment, Fragment[?]]

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
    headerParts.foreach(af => buf += Left(af))
    whereOpt.foreach { f =>
      buf += Left(RawConstants.WHERE)
      buf += Right(f)
    }
    if (groupBys.nonEmpty) {
      buf += Left(RawConstants.GROUP_BY)
      buf += Left(TypedExpr.joined(groupBys.map(e => bindVoid(e.fragment)), ", "))
    }
    havingOpt.foreach { f =>
      buf += Left(RawConstants.HAVING)
      buf += Right(f)
    }
    if (orderBys.nonEmpty) {
      buf += Left(RawConstants.ORDER_BY)
      buf += Left(TypedExpr.joined(orderBys.map(o => bindVoid(o.fragment)), ", "))
    }
    limitOpt.foreach(n => buf += Left(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += Left(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += Left(TypedExpr.raw(" " + l.sql)))
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
   * Glue body parts into a single `Fragment[Concat[A1, A2]]`. Crucially, the final encoder preserves SQL
   * render order — Postgres binds positionally, so the encoded values must come out in the exact `$N`
   * order they appear in the SQL. Mixing baked (Left) and typed (Right) parts and emitting bakeds-first
   * would mis-align with the `$N` slots when typed parts appear inside / between bakeds (e.g. WHERE
   * before ORDER BY where ORDER BY has a Param.bind-bearing CASE WHEN).
   *
   * Solution: a custom encoder that walks the parts list at execute time. Each `Left(af)` contributes
   * its baked `af.argument` through `af.encoder`; each `Right(f)` contributes the user's `Args` (projected
   * via [[Where.Concat2]]) through `f.encoder`. SQL render order ↔ encode order, regardless of mix.
   */
  private[dsl] def assemble[A1, A2, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  )(using c2: Where.Concat2[A1, A2]): QueryTemplate[Where.Concat[A1, A2], R] = {
    // Explicitly thread c2 and a hand-built rightVoid for c123 down to assemble3 — at abstract A1/A2
    // (which is what assemble's body sees), Scala's implicit search picks `default` over the passed-in
    // c2, which then crashes at runtime trying to cast Void to a Tuple2. Passing both evidences
    // explicitly via `using c2, c123` bypasses the search and uses what we know is correct.
    val c123: Where.Concat2[Where.Concat[A1, A2], Void] = new Where.Concat2[Where.Concat[A1, A2], Void] {
      def project(c: Where.Concat[Where.Concat[A1, A2], Void]): (Where.Concat[A1, A2], Void) =
        (c.asInstanceOf[Where.Concat[A1, A2]], Void)
    }
    assemble3[A1, A2, Void, R](bodyParts, ctes, codec)(using c2, c123)
      .asInstanceOf[QueryTemplate[Where.Concat[A1, A2], R]]
  }

  /**
   * Three-slot assemble. Render-order dispatch: each `Right(f)` part is at a *positional* slot in the
   * order it appears among Right parts — the i-th Right slot maps to the i-th of `(A1, A2, A3)`. Void-
   * encoder Right slots still occupy a position (and contribute no encoded value) so the position
   * mapping stays stable when, e.g., UPDATE SET is value-baked but WHERE / RETURNING carry typed Args.
   *
   * Used by SELECT (proj/where/having) and mutation+RETURNING (UPDATE: SET + WHERE + RETURNING). For
   * one- or two-slot cases pass `Void` for the unused trailing positions and emit fewer Right slots —
   * the walker's slot index matches the user's `(A1, A2, A3)` mapping by virtue of the call site
   * emitting Right slots in `(A1, A2, A3)` order.
   */
  private[dsl] def assemble3[A1, A2, A3, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  )(using
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], A3]
  ): QueryTemplate[Where.Concat[Where.Concat[A1, A2], A3], R] = {
    val ctePreamble: Option[AppliedFragment] = renderWithPreamble(ctes)

    // Materialise the parts list with CTE preamble prepended.
    val allParts: List[BodyPart] = ctePreamble.toList.map(Left(_)) ++ bodyParts

    // Concatenate SQL parts in render order.
    val sqlParts: List[Either[String, cats.data.State[Int, String]]] =
      allParts.flatMap {
        case Left(af) => af.fragment.parts
        case Right(f) => f.parts
      }

    type Out = Where.Concat[Where.Concat[A1, A2], A3]
    val finalEnc: Encoder[Out] = new Encoder[Out] {
      override val types: List[skunk.data.Type] =
        allParts.flatMap {
          case Left(af) => af.fragment.encoder.types
          case Right(f) => f.encoder.types
        }
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          allParts.foldLeft((n0, "")) { case ((n, acc), part) =>
            val (n1, s) = part match {
              case Left(af) => af.fragment.encoder.sql.run(n).value
              case Right(f) => f.encoder.sql.run(n).value
            }
            (n1, acc + s)
          }
        }

      override def encode(args: Out): List[Option[skunk.data.Encoded]] = {
        // Project the user's Args into (A1, A2, A3) once via the supplied [[Where.Concat2]] evidences.
        // The walker dispatches each Right slot to a1/a2/a3 by its **positional index** among Right
        // slots — ALWAYS incremented, even for Void-encoder Right slots — so the index stays stable
        // when intermediate slots are baked-Void (e.g. UPDATE SET=value-baked + WHERE Param +
        // RETURNING Param: SET is a Right at idx 0 with Void encoder, WHERE at idx 1, RETURNING at
        // idx 2; positions correctly map to A1=Void / A2=WArgs / A3=RetArgs).
        val (a12, a3v) = c123.project(args)
        val (a1v, a2v) = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])

        var typedIdx = 0
        allParts.flatMap {
          case Left(af) =>
            af.fragment.encoder.asInstanceOf[Encoder[Any]].encode(af.argument)
          case Right(f) =>
            val enc = f.encoder.asInstanceOf[Encoder[Any]]
            val v = typedIdx match {
              case 0 => a1v
              case 1 => a2v
              case _ => a3v
            }
            typedIdx += 1
            if (enc eq Void.codec) Nil
            else enc.encode(v)
        }
      }
    }

    val frag: Fragment[Out] = Fragment(sqlParts, finalEnc, Origin.unknown)
    QueryTemplate.mk[Out, R](frag, codec)
  }

  /**
   * Six-slot assemble — adds one more typed position after A5 (ORDER BY). Used by
   * `ProjectedSelect.compile` for the full SELECT-clause threading: `[DArgs, ProjArgs, WArgs,
   * GArgs, HArgs, OArgs]`.
   */
  private[dsl] def assemble6[A1, A2, A3, A4, A5, A6, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  )(using
    c12:     Where.Concat2[A1, A2],
    c123:    Where.Concat2[Where.Concat[A1, A2], A3],
    c1234:   Where.Concat2[Where.Concat[Where.Concat[A1, A2], A3], A4],
    c12345:  Where.Concat2[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5],
    c123456: Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5], A6]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5], A6], R] = {
    val ctePreamble: Option[AppliedFragment] = renderWithPreamble(ctes)
    val allParts: List[BodyPart] = ctePreamble.toList.map(Left(_)) ++ bodyParts

    val sqlParts: List[Either[String, cats.data.State[Int, String]]] =
      allParts.flatMap {
        case Left(af) => af.fragment.parts
        case Right(f) => f.parts
      }

    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5], A6]
    val finalEnc: Encoder[Out] = new Encoder[Out] {
      override val types: List[skunk.data.Type] =
        allParts.flatMap {
          case Left(af) => af.fragment.encoder.types
          case Right(f) => f.encoder.types
        }
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          allParts.foldLeft((n0, "")) { case ((n, acc), part) =>
            val (n1, s) = part match {
              case Left(af) => af.fragment.encoder.sql.run(n).value
              case Right(f) => f.encoder.sql.run(n).value
            }
            (n1, acc + s)
          }
        }

      override def encode(args: Out): List[Option[skunk.data.Encoded]] = {
        val (a12345, a6v) = c123456.project(args)
        val (a1234, a5v)  = c12345.project(a12345.asInstanceOf[Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5]])
        val (a123, a4v)   = c1234.project(a1234.asInstanceOf[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4]])
        val (a12, a3v)    = c123.project(a123.asInstanceOf[Where.Concat[Where.Concat[A1, A2], A3]])
        val (a1v, a2v)    = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])

        var typedIdx = 0
        allParts.flatMap {
          case Left(af) =>
            af.fragment.encoder.asInstanceOf[Encoder[Any]].encode(af.argument)
          case Right(f) =>
            val enc = f.encoder.asInstanceOf[Encoder[Any]]
            val v = typedIdx match {
              case 0 => a1v
              case 1 => a2v
              case 2 => a3v
              case 3 => a4v
              case 4 => a5v
              case _ => a6v
            }
            typedIdx += 1
            if (enc eq Void.codec) Nil
            else enc.encode(v)
        }
      }
    }

    val frag: Fragment[Out] = Fragment(sqlParts, finalEnc, Origin.unknown)
    QueryTemplate.mk[Out, R](frag, codec)
  }

  /**
   * Five-slot assemble — adds one more typed position before A1. Used by `ProjectedSelect.compile`
   * once DISTINCT ON threads typed `DArgs` ahead of the projection (slots `[DArgs, ProjArgs, WArgs,
   * GArgs, HArgs]`).
   */
  private[dsl] def assemble5[A1, A2, A3, A4, A5, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  )(using
    c12:    Where.Concat2[A1, A2],
    c123:   Where.Concat2[Where.Concat[A1, A2], A3],
    c1234:  Where.Concat2[Where.Concat[Where.Concat[A1, A2], A3], A4],
    c12345: Where.Concat2[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5], R] = {
    val ctePreamble: Option[AppliedFragment] = renderWithPreamble(ctes)
    val allParts: List[BodyPart] = ctePreamble.toList.map(Left(_)) ++ bodyParts

    val sqlParts: List[Either[String, cats.data.State[Int, String]]] =
      allParts.flatMap {
        case Left(af) => af.fragment.parts
        case Right(f) => f.parts
      }

    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], A5]
    val finalEnc: Encoder[Out] = new Encoder[Out] {
      override val types: List[skunk.data.Type] =
        allParts.flatMap {
          case Left(af) => af.fragment.encoder.types
          case Right(f) => f.encoder.types
        }
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          allParts.foldLeft((n0, "")) { case ((n, acc), part) =>
            val (n1, s) = part match {
              case Left(af) => af.fragment.encoder.sql.run(n).value
              case Right(f) => f.encoder.sql.run(n).value
            }
            (n1, acc + s)
          }
        }

      override def encode(args: Out): List[Option[skunk.data.Encoded]] = {
        val (a1234, a5v) = c12345.project(args)
        val (a123, a4v)  = c1234.project(a1234.asInstanceOf[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4]])
        val (a12, a3v)   = c123.project(a123.asInstanceOf[Where.Concat[Where.Concat[A1, A2], A3]])
        val (a1v, a2v)   = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])

        var typedIdx = 0
        allParts.flatMap {
          case Left(af) =>
            af.fragment.encoder.asInstanceOf[Encoder[Any]].encode(af.argument)
          case Right(f) =>
            val enc = f.encoder.asInstanceOf[Encoder[Any]]
            val v = typedIdx match {
              case 0 => a1v
              case 1 => a2v
              case 2 => a3v
              case 3 => a4v
              case _ => a5v
            }
            typedIdx += 1
            if (enc eq Void.codec) Nil
            else enc.encode(v)
        }
      }
    }

    val frag: Fragment[Out] = Fragment(sqlParts, finalEnc, Origin.unknown)
    QueryTemplate.mk[Out, R](frag, codec)
  }

  /**
   * Four-slot assemble — same pattern as [[assemble3]] but with one more typed position. Used by
   * `ProjectedSelect.compile` once GROUP BY threads typed `GArgs` between WHERE and HAVING (slots
   * `[ProjArgs, WArgs, GArgs, HArgs]`). The walker dispatches Right slots positionally to
   * `(a1, a2, a3, a4)`.
   */
  private[dsl] def assemble4[A1, A2, A3, A4, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  )(using
    c12:   Where.Concat2[A1, A2],
    c123:  Where.Concat2[Where.Concat[A1, A2], A3],
    c1234: Where.Concat2[Where.Concat[Where.Concat[A1, A2], A3], A4]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4], R] = {
    val ctePreamble: Option[AppliedFragment] = renderWithPreamble(ctes)
    val allParts: List[BodyPart] = ctePreamble.toList.map(Left(_)) ++ bodyParts

    val sqlParts: List[Either[String, cats.data.State[Int, String]]] =
      allParts.flatMap {
        case Left(af) => af.fragment.parts
        case Right(f) => f.parts
      }

    type Out = Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4]
    val finalEnc: Encoder[Out] = new Encoder[Out] {
      override val types: List[skunk.data.Type] =
        allParts.flatMap {
          case Left(af) => af.fragment.encoder.types
          case Right(f) => f.encoder.types
        }
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          allParts.foldLeft((n0, "")) { case ((n, acc), part) =>
            val (n1, s) = part match {
              case Left(af) => af.fragment.encoder.sql.run(n).value
              case Right(f) => f.encoder.sql.run(n).value
            }
            (n1, acc + s)
          }
        }

      override def encode(args: Out): List[Option[skunk.data.Encoded]] = {
        val (a123, a4v) = c1234.project(args)
        val (a12, a3v)  = c123.project(a123.asInstanceOf[Where.Concat[Where.Concat[A1, A2], A3]])
        val (a1v, a2v)  = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])

        var typedIdx = 0
        allParts.flatMap {
          case Left(af) =>
            af.fragment.encoder.asInstanceOf[Encoder[Any]].encode(af.argument)
          case Right(f) =>
            val enc = f.encoder.asInstanceOf[Encoder[Any]]
            val v = typedIdx match {
              case 0 => a1v
              case 1 => a2v
              case 2 => a3v
              case _ => a4v
            }
            typedIdx += 1
            if (enc eq Void.codec) Nil
            else enc.encode(v)
        }
      }
    }

    val frag: Fragment[Out] = Fragment(sqlParts, finalEnc, Origin.unknown)
    QueryTemplate.mk[Out, R](frag, codec)
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

  def where[A](f: SelectView[Ss] => Where[A])(using
    c2: Where.Concat2[WArgs, A]
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, Where.Concat[WArgs, A], HArgs, Row] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[WArgs, A](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], pred)
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  def whereRaw(af: AppliedFragment)(using
    c2: Where.Concat2[WArgs, Void]
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, ?, HArgs, Row] = {
    val combined = SelectBuilder.andRawInto[WArgs](whereOpt.asInstanceOf[Option[Fragment[WArgs]]], af)
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
    val fresh = f(v) match {
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
    val fresh = f(v) match {
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

  def having[H](f: SelectView[Ss] => Where[H])(using
    c2: Where.Concat2[HArgs, H]
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, Where.Concat[HArgs, H], Row] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto[HArgs, H](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], pred)
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  def havingRaw(af: AppliedFragment)(using
    c2: Where.Concat2[HArgs, Void]
  ): ProjectedSelect[Ss, Proj, Groups, DistinctOn, Orders, WArgs, ?, Row] = {
    val combined = SelectBuilder.andRawInto[HArgs](havingOpt.asInstanceOf[Option[Fragment[HArgs]]], af)
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
    val exprs = f(v) match {
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
   * Bridge for [[Cte]] / `.alias` paths still on AppliedFragment. Renders the body without CTE preamble
   * (outer query's `assemble` owns the WITH). Constrained to Void-args inner queries (`ProjArgs = WArgs
   * = HArgs = Void`); typed-args threading through CTE / `.alias` is roadmap.
   */
  private[dsl] def compileFragment(using @scala.annotation.unused ev: GroupCoverage[Proj, Groups]): AppliedFragment = {
    val distinctSize = distinctOnOpt.fold(0)(_.size)
    val voidDistProjector: Any => List[Any]  = _ => List.fill(distinctSize)(Void)
    val voidProjProjector: Any => List[Any]  = _ => List.fill(projections.size)(Void)
    val voidGroupProjector: Any => List[Any] = _ => List.fill(groupBys.size)(Void)
    val voidOrderProjector: Any => List[Any] = _ => List.fill(orderBys.size)(Void)
    val tpl = SelectBuilder.assemble6[Void, Void, Void, Void, Void, Void, Row](
      bodyParts = compileBodyParts(voidDistProjector, voidProjProjector, voidGroupProjector, voidOrderProjector),
      ctes      = Nil,
      codec     = codec
    )
    tpl.fragment.asInstanceOf[Fragment[Void]].apply(Void)
  }

  /**
   * Compile into a [[QueryTemplate]]. Enforces [[GroupCoverage]] and threads typed `Args` from
   * Param-bearing items in **six** logical positions in render order:
   *
   *   - `DArgs`    — `DISTINCT ON` items (computed via [[ProjArgsOf]] over `DistinctOn`).
   *   - `ProjArgs` — projection items (computed via [[ProjArgsOf]] over `Proj`).
   *   - `WArgs`    — WHERE clause.
   *   - `GArgs`    — GROUP BY items (computed via [[ProjArgsOf]] over `Groups`).
   *   - `HArgs`    — HAVING clause.
   *   - `OArgs`    — ORDER BY items (computed via [[ProjArgsOf]] over `Orders`).
   *
   * Final `Args = Concat[Concat[Concat[Concat[Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs], OArgs]`,
   * with `Void` slots collapsed by [[Where.Concat]].
   */
  def compile[DArgs, ProjArgs, GArgs, OArgs](using
    ev:      GroupCoverage[Proj, Groups],
    d:       ProjArgsOf.Aux[DistinctOn, DArgs],
    pa:      ProjArgsOf.Aux[Proj, ProjArgs],
    g:       ProjArgsOf.Aux[Groups, GArgs],
    o:       ProjArgsOf.Aux[Orders, OArgs],
    c12:     Where.Concat2[DArgs, ProjArgs],
    c123:    Where.Concat2[Where.Concat[DArgs, ProjArgs], WArgs],
    c1234:   Where.Concat2[Where.Concat[Where.Concat[DArgs, ProjArgs], WArgs], GArgs],
    c12345:  Where.Concat2[Where.Concat[Where.Concat[Where.Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs],
    c123456: Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs], OArgs]
  ): QueryTemplate[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DArgs, ProjArgs], WArgs], GArgs], HArgs], OArgs], Row] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val ctes    = collectCtesInOrder(entries)
    // Runtime fallback: typeclass-derived projector returns Nil when the static type doesn't track
    // a clause's items (e.g. `SelectBuilder.groupBy → .select` resets `Groups` to `EmptyTuple`).
    // Size-check and Void-fill so the encoder skips at execute time.
    val rawDistProjector  = d.project.asInstanceOf[Any => List[Any]]
    val rawProjProjector  = pa.project.asInstanceOf[Any => List[Any]]
    val rawGroupProjector = g.project.asInstanceOf[Any => List[Any]]
    val rawOrderProjector = o.project.asInstanceOf[Any => List[Any]]
    val distinctSize      = distinctOnOpt.fold(0)(_.size)
    val distProjector: Any => List[Any] = a => {
      val xs = rawDistProjector(a)
      if (xs.size == distinctSize) xs else List.fill(distinctSize)(Void)
    }
    val projProjector: Any => List[Any] = a => {
      val xs = rawProjProjector(a)
      if (xs.size == projections.size) xs else List.fill(projections.size)(Void)
    }
    val groupProjector: Any => List[Any] = a => {
      val xs = rawGroupProjector(a)
      if (xs.size == groupBys.size) xs else List.fill(groupBys.size)(Void)
    }
    val orderProjector: Any => List[Any] = a => {
      val xs = rawOrderProjector(a)
      if (xs.size == orderBys.size) xs else List.fill(orderBys.size)(Void)
    }
    val parts = compileBodyParts(distProjector, projProjector, groupProjector, orderProjector)
    SelectBuilder.assemble6[DArgs, ProjArgs, WArgs, GArgs, HArgs, OArgs, Row](
      bodyParts = parts,
      ctes      = ctes,
      codec     = codec
    )(using c12, c123, c1234, c12345, c123456)
  }

  /**
   * Build body parts with stable slot indices: `[DIST=0, PROJ=1, WHERE=2, GROUP=3, HAVING=4,
   * ORDER=5]`. Each typed slot is always emitted as a Right — when the corresponding clause is
   * absent, the slot uses an `emptyVoidSlot` placeholder so positions stay aligned with `(A1..A6)`
   * in [[SelectBuilder.assemble6]].
   */
  private def compileBodyParts(
    distProjector:  Any => List[Any],
    projProjector:  Any => List[Any],
    groupProjector: Any => List[Any],
    orderProjector: Any => List[Any]
  ): List[SelectBuilder.BodyPart] = {
    val entries      = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val combinedProj = TypedExpr.combineList[Any](projections.map(_.fragment), ", ", projProjector)
    val buf          = scala.collection.mutable.ListBuffer[BodyPart]()
    // slot 0 = DISTINCT ON — when distinctOnOpt is None, emit `SELECT ` (or `SELECT DISTINCT `) +
    // a placeholder Right with empty content. When set, emit `SELECT DISTINCT ON (` + combined
    // items + `) `.
    distinctOnOpt match {
      case Some(exprs) =>
        val combinedDist = TypedExpr.combineList[Any](exprs.map(_.fragment), ", ", distProjector)
        buf += Left(RawConstants.SELECT_DISTINCT_ON)
        buf += Right(combinedDist)
        buf += Left(RawConstants.CLOSE_PAREN_SPACE)
      case None =>
        buf += Left(if (distinct) RawConstants.SELECT_DISTINCT else RawConstants.SELECT)
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    buf += Right(combinedProj) // slot 1 = PROJ
    if (entries.nonEmpty && entries.head.relation.hasFromClause) {
      val head        = entries.head
      val headerParts = scala.collection.mutable.ListBuffer[AppliedFragment](RawConstants.FROM, aliasedFromEntry(head))
      entries.tail.foreach { s =>
        headerParts += (if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
        headerParts += aliasedFromEntry(s)
        s.onPredOpt.foreach { p =>
          headerParts += RawConstants.ON
          headerParts += SelectBuilder.bindVoid(p.fragment)
        }
      }
      headerParts.foreach(af => buf += Left(af))
    }
    // slot 2 = WHERE
    whereOpt match {
      case Some(f) =>
        buf += Left(RawConstants.WHERE)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 3 = GROUP BY
    if (groupBys.nonEmpty) {
      val combinedGrp = TypedExpr.combineList[Any](groupBys.map(_.fragment), ", ", groupProjector)
      buf += Left(RawConstants.GROUP_BY)
      buf += Right(combinedGrp)
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 4 = HAVING
    havingOpt match {
      case Some(f) =>
        buf += Left(RawConstants.HAVING)
        buf += Right(f)
      case None =>
        buf += Right(SelectBuilder.emptyVoidSlot)
    }
    // slot 5 = ORDER BY — emits the keyword + combined items via the OrderArgs projector. Empty
    // orderBys still emit a placeholder Right slot.
    if (orderBys.nonEmpty) {
      val combinedOrd = TypedExpr.combineList[Any](orderBys.map(_.fragment), ", ", orderProjector)
      buf += Left(RawConstants.ORDER_BY)
      buf += Right(combinedOrd)
    } else {
      buf += Right(SelectBuilder.emptyVoidSlot)
    }
    limitOpt.foreach(n => buf += Left(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += Left(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += Left(TypedExpr.raw(" " + l.sql)))
    buf.toList
  }

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

  def select: SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, Void, Void] = {
    val entry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    new SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, Void, Void](entry *: EmptyTuple)
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
  case SourceEntry[?, ?, c, ?] *: EmptyTuple => ColumnsView[c]
  case _                                     => JoinedView[Ss]
}

private[sharp] def buildSelectView[Ss <: Tuple](sources: Ss): SelectView[Ss] =
  sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]] match {
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
    : IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A] *: EmptyTuple] =
    new IsSingleTable[SourceEntry[Table[Cols, N], Cols, Cols, A] *: EmptyTuple] {}

}

sealed trait IsSingleSource[Ss] {
  type Cols <: Tuple
}

object IsSingleSource {
  type Aux[Ss, C] = IsSingleSource[Ss] { type Cols = C }

  given [RR <: Relation[C], C <: Tuple, A <: String & Singleton]
    : IsSingleSource.Aux[SourceEntry[RR, C, C, A] *: EmptyTuple, C] =
    new IsSingleSource[SourceEntry[RR, C, C, A] *: EmptyTuple] { type Cols = C }

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
