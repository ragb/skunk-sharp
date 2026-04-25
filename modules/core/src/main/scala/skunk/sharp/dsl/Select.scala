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
 * `.compile` produces a `CompiledQuery[Where.Concat[WArgs, HArgs], Row]` — the visible `Args` is the full
 * captured-parameter tuple in SQL render order (WHERE args first, HAVING args second), with `Void`
 * placeholders normalised away by the [[Where.Concat]] match type.
 *
 * Every lambda-taking method receives a [[SelectView]], which the match type reduces to:
 *
 *   - `ColumnsView[Cols]` when there's exactly one source — user writes `u.email`.
 *   - `JoinedView[Ss]` (a Scala 3 named tuple keyed by alias) when there are 2+ sources — user writes
 *     `r.users.email`.
 *
 * Row-level locking (`FOR UPDATE`, `FOR SHARE`, …) is gated on `IsSingleTable[Ss]` evidence — only a
 * single-source query over a [[Table]] can lock; anything else is a compile error.
 *
 * Escape hatches: `.whereRaw(af: AppliedFragment)` and `.havingRaw(af)` accept arbitrary pre-applied
 * fragments and widen the corresponding `Args` slot to `?`. Use these for genuinely runtime-built predicates
 * (user-supplied filter list, ad-hoc extension that doesn't ship a typed `Where`).
 */
final class SelectBuilder[Ss <: Tuple, WArgs, HArgs] private[sharp] (
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean = false,
  private[sharp] val whereOpt: Option[(Fragment[?], Any)] = None,
  private[sharp] val groupBys: List[TypedExpr[?]] = Nil,
  private[sharp] val havingOpt: Option[(Fragment[?], Any)] = None,
  private[sharp] val orderBys: List[OrderBy] = Nil,
  private[sharp] val limitOpt: Option[Int] = None,
  private[sharp] val offsetOpt: Option[Int] = None,
  private[sharp] val lockingOpt: Option[Locking] = None,
  private[sharp] val distinctOnOpt: Option[List[TypedExpr[?]]] = None
) {

  private def cp[W, H](
    distinct: Boolean = distinct,
    whereOpt: Option[(Fragment[?], Any)] = whereOpt,
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[(Fragment[?], Any)] = havingOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt,
    distinctOnOpt: Option[List[TypedExpr[?]]] = distinctOnOpt
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

  /**
   * AND in a typed predicate. New `WArgs` is `Where.Concat[WArgs, A]` — the prior captured args paired with the
   * new ones, with `Void` placeholders normalised away.
   */
  def where[A](f: SelectView[Ss] => Where[A]): SelectBuilder[Ss, Where.Concat[WArgs, A], HArgs] = {
    val pred = f(view)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  /**
   * Escape hatch: AND in a pre-applied `AppliedFragment` whose args are existential. Widens `WArgs` to `?` —
   * subsequent typed `.where` calls re-narrow but the call site sees an existential.
   */
  def whereRaw(af: AppliedFragment): SelectBuilder[Ss, ?, HArgs] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  /** `ORDER BY …` — no runtime params captured at the typed level. */
  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    cp[WArgs, HArgs](orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY …` on a pre-projection builder — runtime-only. Coverage of the later projection isn't type-checked
   * here because we don't know the projection yet.
   */
  def groupBy(f: SelectView[Ss] => TypedExpr[?] | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val fresh = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    cp[WArgs, HArgs](groupBys = groupBys ++ fresh)
  }

  /** `HAVING <typed-predicate>`. Threads `HArgs` similarly to `.where`. */
  def having[H](f: SelectView[Ss] => Where[H]): SelectBuilder[Ss, WArgs, Where.Concat[HArgs, H]] = {
    val pred = f(view)
    val combined = SelectBuilder.andInto(havingOpt, pred)
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  /** Escape hatch HAVING — widens `HArgs` to `?`. */
  def havingRaw(af: AppliedFragment): SelectBuilder[Ss, WArgs, ?] = {
    val combined = SelectBuilder.andRawInto(havingOpt, af)
    cp[WArgs, Any](havingOpt = Some(combined))
  }

  def limit(n: Int): SelectBuilder[Ss, WArgs, HArgs]  = cp[WArgs, HArgs](limitOpt = Some(n))
  def offset(n: Int): SelectBuilder[Ss, WArgs, HArgs] = cp[WArgs, HArgs](offsetOpt = Some(n))

  def distinctRows: SelectBuilder[Ss, WArgs, HArgs] = cp[WArgs, HArgs](distinct = true)

  /**
   * `SELECT DISTINCT ON (e1, e2, …) …` — Postgres-specific distinct-by-subset. Always combine with ORDER BY in
   * practice (per the Postgres docs).
   */
  def distinctOn(f: SelectView[Ss] => TypedExpr[?] | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val exprs = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
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

  /**
   * Attach another source via INNER JOIN. Transitions to [[IncompleteJoin]]; call `.on(...)` to finalise. The
   * new source's alias must not clash with any already-committed source alias (enforced by `AliasNotUsed`).
   */
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

  /** Attach another source via LEFT JOIN. Right-side cols become nullable for subsequent `.where` / `.select`. */
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

  /** Attach another source via RIGHT JOIN — every already-committed source's cols become nullable. */
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

  /** Attach another source via FULL OUTER JOIN — both sides' cols become nullable. */
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

  /** Attach another source via CROSS JOIN. No `.on(...)` required. */
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
   * Projection — pick the columns / expressions to return. Single `TypedExpr[T]` → row is `T`; tuple → row is
   * the tuple of expression output types. Captured args (WHERE / HAVING) thread through to the
   * [[ProjectedSelect]].
   */
  transparent inline def select[X](inline f: SelectView[Ss] => X)
    : ProjectedSelect[Ss, NormProj[X], EmptyTuple, WArgs, HArgs, ProjResult[X]] = {
    val v = view
    f(v) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect[Ss, NormProj[X], EmptyTuple, WArgs, HArgs, ProjResult[X]](
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
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedSelect[Ss, NormProj[X], EmptyTuple, WArgs, HArgs, ProjResult[X]](
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

  transparent inline def apply[X](inline f: SelectView[Ss] => X)
    : ProjectedSelect[Ss, NormProj[X], EmptyTuple, WArgs, HArgs, ProjResult[X]] = select[X](f)

  /**
   * Render the SELECT body. Used by [[cte]] to capture the inner SQL lazily, and by [[compile]]. Returns the
   * full applied fragment with WHERE / HAVING args baked in.
   */
  private[dsl] def compileFragment(using ev: IsSingleSource[Ss]): AppliedFragment = {
    val entries        = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head           = entries.head
    val cols           = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projStr        = cols.map(c => s""""${c.name}"""").mkString(", ")
    val selectPrefix   = renderSelectPrefix(distinct, distinctOnOpt)
    val headerNoSelect =
      if (head.relation.hasFromClause)
        TypedExpr.raw(s"$projStr FROM ") |+| aliasedFromEntry(head)
      else
        TypedExpr.raw(projStr)
    renderClauses(
      selectPrefix |+| headerNoSelect,
      whereOpt,
      groupBys,
      havingOpt,
      orderBys,
      limitOpt,
      offsetOpt,
      lockingOpt
    )
  }

  /**
   * Whole-row `.compile` — only on single-source builders. Returns `CompiledQuery[Args, Row]` where `Args`
   * is `Where.Concat[WArgs, HArgs]` — visible captured-parameter tuple.
   */
  def compile(using ev: IsSingleSource[Ss]): CompiledQuery[Where.Concat[WArgs, HArgs], NamedRowOf[ev.Cols]] = {
    val entries  = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head     = entries.head
    val ctes     = collectCtesInOrder(entries)
    SelectBuilder.assemble[Where.Concat[WArgs, HArgs], NamedRowOf[ev.Cols]](
      bodyParts = compileBodyParts(head),
      ctes = ctes,
      codec = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]
    )
  }

  private def compileBodyParts(head: SourceEntry[?, ?, ?, ?]): List[SelectBuilder.BodyPart] = {
    val cols         = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val projStr      = cols.map(c => s""""${c.name}"""").mkString(", ")
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val headerParts  = scala.collection.mutable.ListBuffer[AppliedFragment](selectPrefix)
    if (head.relation.hasFromClause) {
      headerParts += TypedExpr.raw(s"$projStr FROM ")
      headerParts += aliasedFromEntry(head)
    } else {
      headerParts += TypedExpr.raw(projStr)
    }
    // Tail entries (additional joined sources): keyword + alias + ON predicate go directly into the
    // header parts list. Any ON-predicate args ride along on the predicate's AppliedFragment and
    // surface through assemble's pre-encoder pairing.
    val tail = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    tail.foreach { s =>
      headerParts += (if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
      headerParts += aliasedFromEntry(s)
      s.onPredOpt.foreach { p =>
        headerParts += RawConstants.ON
        headerParts += p.render
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

  /** AND a typed predicate into an existing `Option[(Fragment[?], Any)]` slot, returning a new slot. */
  private[dsl] def andInto[A](slot: Option[(Fragment[?], Any)], pred: Where[A]): (Fragment[?], Any) =
    slot match {
      case None        => (pred.fragment, pred.args)
      case Some((f, a)) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            pred.fragment.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = f.encoder.asInstanceOf[Encoder[Any]].product(pred.fragment.encoder)
        val frag: Fragment[?] = Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
        (frag, (a, pred.args))
    }

  /** AND a pre-applied raw `AppliedFragment` into a slot — bakes its args via contramap. */
  private[dsl] def andRawInto(slot: Option[(Fragment[?], Any)], af: AppliedFragment): (Fragment[?], Any) = {
    // Treat the raw AppliedFragment as a Void-args fragment with values baked via contramap.
    val rawEnc: Encoder[Void] =
      af.fragment.encoder.asInstanceOf[Encoder[Any]].contramap[Void](_ => af.argument)
    val rawFrag: Fragment[Void] = Fragment(af.fragment.parts, rawEnc, Origin.unknown)
    slot match {
      case None        => (rawFrag, Void)
      case Some((f, a)) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            rawFrag.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = f.encoder.asInstanceOf[Encoder[Any]].product(rawEnc)
        val frag: Fragment[?] = Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
        (frag, (a, Void))
    }
  }

  /**
   * Body part — either a pre-applied `AppliedFragment` (raw, args baked in) or a typed `Fragment + args` slot
   * whose encoder must be threaded into the final `Fragment[Args]` so the visible `Args` matches the type-level
   * claim. WHERE / HAVING are the typed slots; everything else (header, ORDER BY, LIMIT, …) is raw.
   */
  private[dsl] type BodyPart = Either[AppliedFragment, (Fragment[?], Any)]

  /**
   * Build the body-parts list for a SELECT or projected SELECT in render order.
   *
   * `headerParts` is the SELECT/FROM/JOIN section flattened into individual `AppliedFragment`s — each becomes a
   * `Left(_)` body part directly, skipping the throwaway intermediate `Fragment + AppliedFragment` allocations
   * that a `|+|` chain would produce. `assemble` flattens all `Left(_)` parts into one final `Fragment[Args]`,
   * so splitting the header costs nothing at the SQL-output level.
   */
  private[dsl] def bodyPartsAround(
    headerParts: List[AppliedFragment],
    whereOpt: Option[(Fragment[?], Any)],
    groupBys: List[TypedExpr[?]],
    havingOpt: Option[(Fragment[?], Any)],
    orderBys: List[OrderBy],
    limitOpt: Option[Int],
    offsetOpt: Option[Int],
    lockingOpt: Option[Locking]
  ): List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart]()
    headerParts.foreach(af => buf += Left(af))
    whereOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.WHERE)
      buf += Right((f, a))
    }
    if (groupBys.nonEmpty) {
      buf += Left(RawConstants.GROUP_BY)
      buf += Left(TypedExpr.joined(groupBys.map(_.render), ", "))
    }
    havingOpt.foreach { case (f, a) =>
      buf += Left(RawConstants.HAVING)
      buf += Right((f, a))
    }
    if (orderBys.nonEmpty) {
      buf += Left(RawConstants.ORDER_BY)
      buf += Left(TypedExpr.joined(orderBys.map(_.af), ", "))
    }
    limitOpt.foreach(n => buf += Left(TypedExpr.raw(s" LIMIT $n")))
    offsetOpt.foreach(n => buf += Left(TypedExpr.raw(s" OFFSET $n")))
    lockingOpt.foreach(l => buf += Left(TypedExpr.raw(" " + l.sql)))
    buf.toList
  }

  /**
   * Glue body parts into a single `Fragment[Args]`. Structural parts are concatenated in render order.
   * Encoder is built as: `preEncoder.product(typedEncoder).contramap[Args](a => (preArgs, a))` — non-typed
   * AppliedFragment args are pre-applied via contramap; typed slots compose into the visible `Args`.
   */
  private[dsl] def assemble[Args, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  ): CompiledQuery[Args, R] = {
    val ctePartsOpt = renderWithPreamble(ctes)
    val cteParts: List[Either[String, cats.data.State[Int, String]]] =
      ctePartsOpt.fold(Nil: List[Either[String, cats.data.State[Int, String]]])(_.fragment.parts)

    // Concat all structural parts in order.
    val structuralBuf = scala.collection.mutable.ListBuffer[Either[String, cats.data.State[Int, String]]]()
    structuralBuf ++= cteParts
    bodyParts.foreach {
      case Left(af)      => structuralBuf ++= af.fragment.parts
      case Right((f, _)) => structuralBuf ++= f.parts
    }
    val structural = structuralBuf.toList

    // Pre-applied (non-typed) encoder + args, accumulated from Left(af) entries with non-Void encoders.
    var preEnc: Encoder[Any] = Void.codec.asInstanceOf[Encoder[Any]]
    var preArgs: Any         = Void
    if (ctePartsOpt.isDefined) {
      val cteAf = ctePartsOpt.get
      val nextE = cteAf.fragment.encoder.asInstanceOf[Encoder[Any]]
      if (!(nextE eq Void.codec)) {
        preEnc = nextE
        preArgs = cteAf.argument
      }
    }
    bodyParts.foreach {
      case Left(af) =>
        val nextE = af.fragment.encoder.asInstanceOf[Encoder[Any]]
        if (!(nextE eq Void.codec)) {
          if (preEnc eq Void.codec) {
            preEnc = nextE
            preArgs = af.argument
          } else {
            preEnc = preEnc.product(nextE).asInstanceOf[Encoder[Any]]
            preArgs = (preArgs, af.argument)
          }
        }
      case Right(_) => () // typed slot — handled below
    }

    // Typed encoder + args from WHERE / HAVING (Right entries). There may be 0, 1, or 2.
    var typedEnc: Encoder[Any] = Void.codec.asInstanceOf[Encoder[Any]]
    var typedArgs: Any         = Void
    bodyParts.foreach {
      case Right((f, a)) =>
        val nextE = f.encoder.asInstanceOf[Encoder[Any]]
        if (typedEnc eq Void.codec) {
          typedEnc = nextE
          typedArgs = a
        } else {
          typedEnc = typedEnc.product(nextE).asInstanceOf[Encoder[Any]]
          typedArgs = (typedArgs, a)
        }
      case _ => ()
    }

    // Final encoder takes Args (the visible typed-args type) and produces the value list. If there are no
    // typed args, contramap from Args=Void → preArgs. Otherwise pair preArgs with the Args input.
    val combinedEnc: Encoder[Any] =
      if ((preEnc eq Void.codec) && (typedEnc eq Void.codec)) Void.codec.asInstanceOf[Encoder[Any]]
      else if (preEnc eq Void.codec)                         typedEnc
      else if (typedEnc eq Void.codec)                       preEnc.contramap[Any](_ => preArgs)
      else                                                    preEnc.product(typedEnc).contramap[Any](a => (preArgs, a))

    val combinedArgs: Any = if (typedEnc eq Void.codec) Void else typedArgs

    val frag: Fragment[Args] = Fragment(structural, combinedEnc, Origin.unknown).asInstanceOf[Fragment[Args]]
    CompiledQuery.mk[Args, R](frag, combinedArgs.asInstanceOf[Args], codec)
  }

}

/**
 * A SELECT with an explicit projection list — rows have shape `Row` instead of the relation's default named
 * tuple. Threads `WArgs` / `HArgs` so the captured-parameter tuple surfaces at `.compile`.
 */
final class ProjectedSelect[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, WArgs, HArgs, Row](
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean,
  private[sharp] val projections: List[TypedExpr[?]],
  private[sharp] val codec: Codec[Row],
  private[sharp] val whereOpt: Option[(Fragment[?], Any)],
  private[sharp] val groupBys: List[TypedExpr[?]],
  private[sharp] val havingOpt: Option[(Fragment[?], Any)],
  private[sharp] val orderBys: List[OrderBy],
  private[sharp] val limitOpt: Option[Int],
  private[sharp] val offsetOpt: Option[Int],
  private[sharp] val lockingOpt: Option[Locking] = None,
  private[sharp] val distinctOnOpt: Option[List[TypedExpr[?]]] = None
) {

  private def cp[W, H](
    distinct: Boolean = distinct,
    projections: List[TypedExpr[?]] = projections,
    codec: Codec[Row] = codec,
    whereOpt: Option[(Fragment[?], Any)] = whereOpt,
    groupBys: List[TypedExpr[?]] = groupBys,
    havingOpt: Option[(Fragment[?], Any)] = havingOpt,
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt,
    distinctOnOpt: Option[List[TypedExpr[?]]] = distinctOnOpt
  ): ProjectedSelect[Ss, Proj, Groups, W, H, Row] =
    new ProjectedSelect[Ss, Proj, Groups, W, H, Row](
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

  def where[A](f: SelectView[Ss] => Where[A])
      : ProjectedSelect[Ss, Proj, Groups, Where.Concat[WArgs, A], HArgs, Row] = {
    val pred = f(view)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  def whereRaw(af: AppliedFragment): ProjectedSelect[Ss, Proj, Groups, ?, HArgs, Row] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    cp[WArgs, HArgs](orderBys = orderBys ++ fresh)
  }

  /**
   * `GROUP BY …`. `G` is captured as a phantom — bare-column elements contribute their names to [[Groups]] for
   * the eventual coverage check at `.compile` time. Accumulates across multiple calls via `Tuple.Concat`.
   */
  transparent inline def groupBy[G](inline f: SelectView[Ss] => G)
    : ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], WArgs, HArgs, Row] = {
    val v     = view
    val fresh = f(v) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    new ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], WArgs, HArgs, Row](
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

  def having[H](f: SelectView[Ss] => Where[H])
      : ProjectedSelect[Ss, Proj, Groups, WArgs, Where.Concat[HArgs, H], Row] = {
    val pred = f(view)
    val combined = SelectBuilder.andInto(havingOpt, pred)
    cp[WArgs, Where.Concat[HArgs, H]](havingOpt = Some(combined))
  }

  def havingRaw(af: AppliedFragment): ProjectedSelect[Ss, Proj, Groups, WArgs, ?, Row] = {
    val combined = SelectBuilder.andRawInto(havingOpt, af)
    cp[WArgs, Any](havingOpt = Some(combined))
  }

  def limit(n: Int): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row]  = cp[WArgs, HArgs](limitOpt = Some(n))
  def offset(n: Int): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] = cp[WArgs, HArgs](offsetOpt = Some(n))

  def distinctRows: ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] = cp[WArgs, HArgs](distinct = true)

  def distinctOn(f: SelectView[Ss] => TypedExpr[?] | Tuple)
      : ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] = {
    val exprs = f(view) match {
      case e: TypedExpr[?] => List(e)
      case t: Tuple        => t.toList.asInstanceOf[List[TypedExpr[?]]]
    }
    cp[WArgs, HArgs](distinctOnOpt = Some(exprs))
  }

  def forUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForUpdate)))

  def forNoKeyUpdate(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForNoKeyUpdate)))

  def forShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForShare)))

  def forKeyShare(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = Some(Locking(LockMode.ForKeyShare)))

  def skipLocked(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.SkipLocked)))

  def noWait(using ev: IsSingleTable[Ss]): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] =
    cp[WArgs, HArgs](lockingOpt = lockingOpt.map(_.copy(waitPolicy = WaitPolicy.NoWait)))

  /** Lift result rows into a case class `T`. */
  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] { type MirroredElemTypes = Row & Tuple }
  ): ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, T] = {
    val newCodec: Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[Row]
    )
    new ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, T](
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

  /** Render the SELECT body as an AppliedFragment. Used by `cte` and by `compile`. */
  private[dsl] def compileFragment(using @scala.annotation.unused ev: GroupCoverage[Proj, Groups]): AppliedFragment = {
    val entries      = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val projList     = TypedExpr.joined(projections.map(_.render), ", ")
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val header       =
      if (entries.isEmpty || !entries.head.relation.hasFromClause)
        selectPrefix |+| projList
      else {
        val head     = entries.head
        val headFrag = selectPrefix |+| projList |+| RawConstants.FROM |+| aliasedFromEntry(head)
        entries.tail.foldLeft(headFrag) { (acc, s) =>
          val kindAf   = if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf
          val fromFrag = kindAf |+| aliasedFromEntry(s)
          s.onPredOpt.fold(acc |+| fromFrag)(p =>
            acc |+| fromFrag |+| RawConstants.ON |+| p.render
          )
        }
      }
    renderClauses(header, whereOpt, groupBys, havingOpt, orderBys, limitOpt, offsetOpt, lockingOpt)
  }

  /** Compile into a [[CompiledQuery]]. Enforces [[GroupCoverage]]. */
  def compile(using ev: GroupCoverage[Proj, Groups]): CompiledQuery[Where.Concat[WArgs, HArgs], Row] = {
    val entries  = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val ctes     = collectCtesInOrder(entries)
    val bodyAfs  = compileBodyParts()
    SelectBuilder.assemble[Where.Concat[WArgs, HArgs], Row](
      bodyParts = bodyAfs,
      ctes = ctes,
      codec = codec
    )
  }

  private def compileBodyParts(): List[SelectBuilder.BodyPart] = {
    val entries      = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val projList     = TypedExpr.joined(projections.map(_.render), ", ")
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val headerParts  = scala.collection.mutable.ListBuffer[AppliedFragment](selectPrefix, projList)
    if (entries.nonEmpty && entries.head.relation.hasFromClause) {
      val head = entries.head
      headerParts += RawConstants.FROM
      headerParts += aliasedFromEntry(head)
      entries.tail.foreach { s =>
        headerParts += (if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
        headerParts += aliasedFromEntry(s)
        s.onPredOpt.foreach { p =>
          headerParts += RawConstants.ON
          headerParts += p.render
        }
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

/**
 * Render the `SELECT`-keyword prefix with trailing space.
 */
private[dsl] def renderSelectPrefix(
  distinct: Boolean,
  distinctOnOpt: Option[List[TypedExpr[?]]]
): skunk.AppliedFragment = {
  import skunk.sharp.internal.RawConstants.*
  distinctOnOpt match {
    case Some(exprs) =>
      SELECT_DISTINCT_ON |+|
        TypedExpr.joined(exprs.map(_.render), ", ") |+|
        CLOSE_PAREN_SPACE
    case None =>
      if (distinct) SELECT_DISTINCT
      else SELECT
  }
}

/**
 * Shared render helper — takes WHERE / HAVING slots in their existential form and renders them as
 * AppliedFragments (via `fragment(args)`). Used by `compileFragment` (the AppliedFragment path used by CTEs and
 * subquery embedding). The Args-tracking compile path uses [[SelectBuilder.assemble]] instead.
 */
private[dsl] def renderClauses(
  header: skunk.AppliedFragment,
  whereOpt: Option[(Fragment[?], Any)],
  groupBys: List[TypedExpr[?]],
  havingOpt: Option[(Fragment[?], Any)],
  orderBys: List[OrderBy],
  limitOpt: Option[Int],
  offsetOpt: Option[Int],
  lockingOpt: Option[Locking]
): skunk.AppliedFragment = {
  import skunk.sharp.internal.RawConstants.*
  val withWhere = whereOpt.fold(header) { case (f, a) =>
    val applied = f.asInstanceOf[Fragment[Any]].apply(a)
    header |+| WHERE |+| applied
  }
  val withGroup =
    if (groupBys.isEmpty) withWhere
    else withWhere |+| GROUP_BY |+| TypedExpr.joined(groupBys.map(_.render), ", ")
  val withHaving = havingOpt.fold(withGroup) { case (f, a) =>
    val applied = f.asInstanceOf[Fragment[Any]].apply(a)
    withGroup |+| HAVING |+| applied
  }
  val withOrder  =
    if (orderBys.isEmpty) withHaving
    else withHaving |+| ORDER_BY |+| TypedExpr.joined(orderBys.map(_.af), ", ")
  val withLimit  = limitOpt.fold(withOrder)(n => withOrder |+| TypedExpr.raw(s" LIMIT $n"))
  val withOffset = offsetOpt.fold(withLimit)(n => withLimit |+| TypedExpr.raw(s" OFFSET $n"))
  lockingOpt.fold(withOffset)(l => withOffset |+| TypedExpr.raw(" " + l.sql))
}

// ---- Entry points -----------------------------------------------------------------------------

/**
 * `.select` on any relation-like value. Produces a single-source [[SelectBuilder]] with `WArgs = Void` and
 * `HArgs = Void`.
 */
extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton, ML <: AliasMode](left: L)(using
  aL: AsRelation.Aux[L, RL, CL, AL, ML]
) {

  def select: SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, Void, Void] = {
    val entry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    new SelectBuilder[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, Void, Void](entry *: EmptyTuple)
  }

}

/**
 * `empty.select(…)` — FROM-less SELECT.
 */
extension (rel: skunk.sharp.empty.type) {

  /** FROM-less SELECT of a single expression. */
  def select[T](e: TypedExpr[T]): ProjectedSelect[EmptyTuple, TypedExpr[T] *: EmptyTuple, EmptyTuple, Void, Void, T] =
    new ProjectedSelect[EmptyTuple, TypedExpr[T] *: EmptyTuple, EmptyTuple, Void, Void, T](
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

  /** FROM-less SELECT of a tuple of expressions. */
  def select[X <: NonEmptyTuple](t: X): ProjectedSelect[EmptyTuple, X, EmptyTuple, Void, Void, ExprOutputs[X]] = {
    val exprs = t.toList.asInstanceOf[List[TypedExpr[?]]]
    val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ExprOutputs[X]]]
    new ProjectedSelect[EmptyTuple, X, EmptyTuple, Void, Void, ExprOutputs[X]](
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

  transparent inline def select[X](inline f: ColumnsView[EmptyTuple] => X)
    : ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, Void, Void, ProjResult[X]] = {
    val v = ColumnsView(EmptyTuple)
    f(v) match {
      case expr: TypedExpr[?] =>
        new ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, Void, Void, ProjResult[X]](
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
      case tup: NonEmptyTuple =>
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?]]]
        val codec = tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[ProjResult[X]]]
        new ProjectedSelect[EmptyTuple, NormProj[X], EmptyTuple, Void, Void, ProjResult[X]](
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
      if (single.alias == single.relation.name)
        ColumnsView(single.effectiveCols).asInstanceOf[SelectView[Ss]]
      else
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
  case EmptyTuple           => EmptyTuple
  case TypedExpr[t] *: tail => t *: ExprOutputs[tail]
}

type ProjResult[X] = X match {
  case TypedExpr[t]  => t
  case NonEmptyTuple => ExprOutputs[X & NonEmptyTuple]
}

type NormProj[X] <: Tuple = X match {
  case NonEmptyTuple => X & Tuple
  case _             => X *: EmptyTuple
}

type LookupTypes[Cols <: Tuple, Names <: Tuple] <: Tuple = Names match {
  case EmptyTuple => EmptyTuple
  case n *: rest  => ColumnType[Cols, n & String & Singleton] *: LookupTypes[Cols, rest]
}

final case class OrderBy(af: skunk.AppliedFragment) {
  def nullsFirst: OrderBy = OrderBy(af |+| TypedExpr.raw(" NULLS FIRST"))
  def nullsLast: OrderBy  = OrderBy(af |+| TypedExpr.raw(" NULLS LAST"))
}

extension [T](expr: TypedExpr[T]) {
  def asc: OrderBy  = OrderBy(expr.render |+| TypedExpr.raw(" ASC"))
  def desc: OrderBy = OrderBy(expr.render |+| TypedExpr.raw(" DESC"))
}
