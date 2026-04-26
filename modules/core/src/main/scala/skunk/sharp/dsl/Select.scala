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
  private[sharp] val orderBys: List[OrderBy] = Nil,
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
    orderBys: List[OrderBy] = orderBys,
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
  def where[A](f: SelectView[Ss] => Where[A]): SelectBuilder[Ss, Where.Concat[WArgs, A], HArgs] = {
    val pred     = f(view)
    val combined = SelectBuilder.andInto(whereOpt, pred)
    cp[Where.Concat[WArgs, A], HArgs](whereOpt = Some(combined))
  }

  /** Escape hatch — widens `WArgs` to `?`. */
  def whereRaw(af: AppliedFragment): SelectBuilder[Ss, ?, HArgs] = {
    val combined = SelectBuilder.andRawInto(whereOpt, af)
    cp[Any, HArgs](whereOpt = Some(combined))
  }

  /** `ORDER BY …` — typed exprs may carry their own Args; absorbed into the assembled fragment encoder. */
  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): SelectBuilder[Ss, WArgs, HArgs] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
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
  def having[H](f: SelectView[Ss] => Where[H]): SelectBuilder[Ss, WArgs, Where.Concat[HArgs, H]] = {
    val pred     = f(view)
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

  transparent inline def select[X](inline f: SelectView[Ss] => X)
    : ProjectedSelect[Ss, NormProj[X], EmptyTuple, WArgs, HArgs, ProjResult[X]] = {
    val v = view
    f(v) match {
      case expr: TypedExpr[?, ?] =>
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
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?, ?]]]
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
   * Bridge for [[Cte]] / `SelectBuilder.alias` that still need an `AppliedFragment`. Renders the SELECT body
   * **without** the CTE preamble (the outer query's `assemble` is responsible for emitting `WITH` once).
   * Currently constrained to `WArgs = Void` and `HArgs = Void` — typed-args threading through CTE bodies and
   * aliased subqueries is roadmap.
   */
  private[dsl] def compileFragment(using ev: IsSingleSource[Ss]): AppliedFragment = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head
    val tpl     = SelectBuilder.assemble[Void, NamedRowOf[ev.Cols]](
      bodyParts = compileBodyParts(head),
      ctes      = Nil, // outer query owns the WITH preamble
      codec     = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]
    )
    tpl.fragment.apply(Void)
  }

  /**
   * Whole-row `.compile` — only on single-source builders. Returns `QueryTemplate[Args, Row]` where `Args`
   * is `Where.Concat[WArgs, HArgs]`.
   */
  def compile(using ev: IsSingleSource[Ss]): QueryTemplate[Where.Concat[WArgs, HArgs], NamedRowOf[ev.Cols]] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head
    val ctes    = collectCtesInOrder(entries)
    SelectBuilder.assemble[Where.Concat[WArgs, HArgs], NamedRowOf[ev.Cols]](
      bodyParts = compileBodyParts(head),
      ctes      = ctes,
      codec     = rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]
    )
  }

  private def compileBodyParts(head: SourceEntry[?, ?, ?, ?]): List[SelectBuilder.BodyPart] = {
    val rel          = head.relation
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val headerParts  = scala.collection.mutable.ListBuffer[Fragment[?]](TypedExpr.liftAfToVoid(selectPrefix))
    val canCacheCols = (head.effectiveCols eq rel.columns) && (head.alias == rel.currentAlias)
    if (rel.hasFromClause) {
      if (canCacheCols) {
        rel.starProjFromAfOpt match {
          case Some(af) => headerParts += TypedExpr.liftAfToVoid(af)
          case None =>
            headerParts += TypedExpr.liftAfToVoid(rel.starProjAf)
            headerParts += TypedExpr.liftAfToVoid(RawConstants.FROM)
            headerParts += TypedExpr.liftAfToVoid(aliasedFromEntry(head))
        }
      } else {
        val cols    = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
        val projStr = cols.map(c => s""""${c.name}"""").mkString(", ")
        headerParts += TypedExpr.voidFragment(s"$projStr FROM ")
        headerParts += TypedExpr.liftAfToVoid(aliasedFromEntry(head))
      }
    } else {
      if (canCacheCols) {
        headerParts += TypedExpr.liftAfToVoid(rel.starProjAf)
      } else {
        val cols    = head.effectiveCols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
        val projStr = cols.map(c => s""""${c.name}"""").mkString(", ")
        headerParts += TypedExpr.voidFragment(projStr)
      }
    }
    val tail = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].tail
    tail.foreach { s =>
      headerParts += TypedExpr.liftAfToVoid(if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
      headerParts += TypedExpr.liftAfToVoid(aliasedFromEntry(s))
      s.onPredOpt.foreach { p =>
        headerParts += TypedExpr.liftAfToVoid(RawConstants.ON)
        headerParts += p.fragment
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

  /** AND a typed predicate into an existing slot. The output `Fragment[?]` carries the combined args via product. */
  private[dsl] def andInto[A](slot: Option[Fragment[?]], pred: Where[A]): Fragment[?] =
    slot match {
      case None    => pred.fragment
      case Some(f) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            pred.fragment.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = combineEncoders(f.encoder, pred.fragment.encoder)
        Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
    }

  /** AND a pre-applied raw `AppliedFragment` into a slot — bakes its args via contramap. */
  private[dsl] def andRawInto(slot: Option[Fragment[?]], af: AppliedFragment): Fragment[?] = {
    val rawFrag: Fragment[Void] = TypedExpr.liftAfToVoid(af)
    slot match {
      case None    => rawFrag
      case Some(f) =>
        val parts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            f.parts ++
            RawConstants.AND.fragment.parts ++
            rawFrag.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val enc = combineEncoders(f.encoder, rawFrag.encoder)
        Fragment(parts, enc, Origin.unknown).asInstanceOf[Fragment[?]]
    }
  }

  /** Void-aware product of two encoders. Returns `Encoder[Any]`; caller casts at the Fragment construction site. */
  private[dsl] def combineEncoders(a: Encoder[?], b: Encoder[?]): Encoder[Any] = {
    val voidLeft  = a eq Void.codec
    val voidRight = b eq Void.codec
    if (voidLeft && voidRight) Void.codec.asInstanceOf[Encoder[Any]]
    else if (voidLeft)         b.asInstanceOf[Encoder[Any]]
    else if (voidRight)        a.asInstanceOf[Encoder[Any]]
    else                       a.asInstanceOf[Encoder[Any]].product(b.asInstanceOf[Encoder[Any]]).asInstanceOf[Encoder[Any]]
  }

  /**
   * Body-part — just a `Fragment[?]`. Each part carries its own encoder (typed slots have a typed encoder; structural
   * pieces have `Void.codec`). [[assemble]] product-combines them into the final `Fragment[Args]`.
   */
  private[dsl] type BodyPart = Fragment[?]

  /**
   * Build the body-parts list for a SELECT or projected SELECT in render order.
   */
  private[dsl] def bodyPartsAround(
    headerParts: List[Fragment[?]],
    whereOpt:    Option[Fragment[?]],
    groupBys:    List[TypedExpr[?, ?]],
    havingOpt:   Option[Fragment[?]],
    orderBys:    List[OrderBy],
    limitOpt:    Option[Int],
    offsetOpt:   Option[Int],
    lockingOpt:  Option[Locking]
  ): List[BodyPart] = {
    val buf = scala.collection.mutable.ListBuffer[BodyPart]()
    headerParts.foreach(buf += _)
    whereOpt.foreach { f =>
      buf += TypedExpr.liftAfToVoid(RawConstants.WHERE)
      buf += f
    }
    if (groupBys.nonEmpty) {
      buf += TypedExpr.liftAfToVoid(RawConstants.GROUP_BY)
      buf ++= joinFragments(groupBys.map(_.fragment), ", ")
    }
    havingOpt.foreach { f =>
      buf += TypedExpr.liftAfToVoid(RawConstants.HAVING)
      buf += f
    }
    if (orderBys.nonEmpty) {
      buf += TypedExpr.liftAfToVoid(RawConstants.ORDER_BY)
      buf ++= joinFragments(orderBys.map(_.fragment), ", ")
    }
    limitOpt.foreach(n => buf += TypedExpr.liftAfToVoid(RawConstants.limitAf(n)))
    offsetOpt.foreach(n => buf += TypedExpr.liftAfToVoid(RawConstants.offsetAf(n)))
    lockingOpt.foreach(l => buf += TypedExpr.voidFragment(" " + l.sql))
    buf.toList
  }

  /** Interleave `parts` with `Void`-fragment separators: `[p0, sep, p1, sep, …, pN]`. */
  private[dsl] def joinFragments(parts: List[Fragment[?]], sep: String): List[Fragment[?]] =
    parts match {
      case Nil          => Nil
      case head :: Nil  => head :: Nil
      case head :: tail =>
        val sepFrag: Fragment[?] = TypedExpr.voidFragment(sep)
        val buf = scala.collection.mutable.ListBuffer[Fragment[?]](head)
        tail.foreach { p =>
          buf += sepFrag
          buf += p
        }
        buf.toList
    }

  /**
   * Glue body parts into a single `Fragment[Args]`. Walks parts in order, accumulating concatenated SQL parts and
   * Void-aware product-combined encoder. The visible `Args` is the caller's claim; the runtime cast aligns the
   * accumulated encoder shape against it.
   */
  private[dsl] def assemble[Args, R](
    bodyParts: List[BodyPart],
    ctes:      List[CteRelation[?, ?]],
    codec:     Codec[R]
  ): QueryTemplate[Args, R] = {
    val ctePartsOpt: Option[Fragment[Void]] = renderWithPreamble(ctes).map(TypedExpr.liftAfToVoid)

    val partsBuf = scala.collection.mutable.ListBuffer[Either[String, cats.data.State[Int, String]]]()
    var enc: Encoder[Any] = Void.codec.asInstanceOf[Encoder[Any]]

    def addFragment(f: Fragment[?]): Unit = {
      partsBuf ++= f.parts
      val nextE = f.encoder.asInstanceOf[Encoder[Any]]
      if (!(nextE eq Void.codec)) {
        if (enc eq Void.codec) enc = nextE
        else                    enc = enc.product(nextE).asInstanceOf[Encoder[Any]]
      }
    }

    ctePartsOpt.foreach(addFragment)
    bodyParts.foreach(addFragment)

    val frag: Fragment[Args] = Fragment(partsBuf.toList, enc, Origin.unknown).asInstanceOf[Fragment[Args]]
    QueryTemplate.mk[Args, R](frag, codec)
  }

}

/**
 * A SELECT with an explicit projection list — rows have shape `Row` instead of the relation's default named tuple.
 */
final class ProjectedSelect[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, WArgs, HArgs, Row](
  private[sharp] val sources: Ss,
  private[sharp] val distinct: Boolean,
  private[sharp] val projections: List[TypedExpr[?, ?]],
  private[sharp] val codec: Codec[Row],
  private[sharp] val whereOpt: Option[Fragment[?]],
  private[sharp] val groupBys: List[TypedExpr[?, ?]],
  private[sharp] val havingOpt: Option[Fragment[?]],
  private[sharp] val orderBys: List[OrderBy],
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
    orderBys: List[OrderBy] = orderBys,
    limitOpt: Option[Int] = limitOpt,
    offsetOpt: Option[Int] = offsetOpt,
    lockingOpt: Option[Locking] = lockingOpt,
    distinctOnOpt: Option[List[TypedExpr[?, ?]]] = distinctOnOpt
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
    val pred     = f(view)
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

  transparent inline def groupBy[G](inline f: SelectView[Ss] => G)
    : ProjectedSelect[Ss, Proj, Tuple.Concat[Groups, NormProj[G]], WArgs, HArgs, Row] = {
    val v     = view
    val fresh = f(v) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
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
    val pred     = f(view)
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

  def distinctOn(f: SelectView[Ss] => TypedExpr[?, ?] | Tuple)
      : ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row] = {
    val exprs = f(view) match {
      case e: TypedExpr[?, ?] => List(e)
      case t: Tuple           => t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
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

  /**
   * Bridge for [[Cte]] / `.alias` paths still on AppliedFragment. Renders the body without CTE preamble
   * (outer query's `assemble` owns the WITH). Constrained to Void-args inner queries.
   */
  private[dsl] def compileFragment(using @scala.annotation.unused ev: GroupCoverage[Proj, Groups]): AppliedFragment = {
    val tpl = SelectBuilder.assemble[Void, Row](
      bodyParts = compileBodyParts(),
      ctes      = Nil,
      codec     = codec
    )
    tpl.fragment.apply(Void)
  }

  /** Compile into a [[QueryTemplate]]. Enforces [[GroupCoverage]]. */
  def compile(using ev: GroupCoverage[Proj, Groups]): QueryTemplate[Where.Concat[WArgs, HArgs], Row] = {
    val entries = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val ctes    = collectCtesInOrder(entries)
    val parts   = compileBodyParts()
    SelectBuilder.assemble[Where.Concat[WArgs, HArgs], Row](
      bodyParts = parts,
      ctes      = ctes,
      codec     = codec
    )
  }

  private def compileBodyParts(): List[SelectBuilder.BodyPart] = {
    val entries      = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val selectPrefix = renderSelectPrefix(distinct, distinctOnOpt)
    val projParts    = SelectBuilder.joinFragments(projections.map(_.fragment), ", ")
    val headerParts  = scala.collection.mutable.ListBuffer[Fragment[?]](TypedExpr.liftAfToVoid(selectPrefix))
    headerParts ++= projParts
    if (entries.nonEmpty && entries.head.relation.hasFromClause) {
      val head = entries.head
      headerParts += TypedExpr.liftAfToVoid(RawConstants.FROM)
      headerParts += TypedExpr.liftAfToVoid(aliasedFromEntry(head))
      entries.tail.foreach { s =>
        headerParts += TypedExpr.liftAfToVoid(if (s.isLateral) s.kind.lateralKeywordAf else s.kind.keywordAf)
        headerParts += TypedExpr.liftAfToVoid(aliasedFromEntry(s))
        s.onPredOpt.foreach { p =>
          headerParts += TypedExpr.liftAfToVoid(RawConstants.ON)
          headerParts += p.fragment
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

  def select[T, A](e: TypedExpr[T, A]): ProjectedSelect[EmptyTuple, TypedExpr[T, A] *: EmptyTuple, EmptyTuple, Void, Void, T] =
    new ProjectedSelect[EmptyTuple, TypedExpr[T, A] *: EmptyTuple, EmptyTuple, Void, Void, T](
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

  def select[X <: NonEmptyTuple](t: X): ProjectedSelect[EmptyTuple, X, EmptyTuple, Void, Void, ExprOutputs[X]] = {
    val exprs = t.toList.asInstanceOf[List[TypedExpr[?, ?]]]
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
      case expr: TypedExpr[?, ?] =>
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
        val exprs = tup.toList.asInstanceOf[List[TypedExpr[?, ?]]]
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

/** ORDER BY entry — typed `Fragment[?]` so Param-bearing exprs (`Param[Int].desc`) thread Args into the assembled query. */
final case class OrderBy(fragment: Fragment[?]) {
  def nullsFirst: OrderBy = OrderBy(appendVoid(fragment, " NULLS FIRST"))
  def nullsLast: OrderBy  = OrderBy(appendVoid(fragment, " NULLS LAST"))
  private def appendVoid(f: Fragment[?], s: String): Fragment[?] = {
    val parts = f.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s))
    Fragment(parts, f.encoder, Origin.unknown).asInstanceOf[Fragment[?]]
  }
}

extension [T, A](expr: TypedExpr[T, A]) {
  def asc: OrderBy  = OrderBy(appendVoid(expr.fragment, " ASC"))
  def desc: OrderBy = OrderBy(appendVoid(expr.fragment, " DESC"))
  private def appendVoid(f: Fragment[?], s: String): Fragment[?] = {
    val parts = f.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s))
    Fragment(parts, f.encoder, Origin.unknown).asInstanceOf[Fragment[?]]
  }
}
