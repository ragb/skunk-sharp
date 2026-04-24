package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment}
import skunk.sharp.{NamedRowOf, TypedExpr}
import skunk.sharp.internal.{rowCodec, RawConstants, TypedWhere}
import skunk.util.Origin

/**
 * Typed SELECT chain — a full builder that carries an `Args` type parameter, distinct from the untyped
 * [[SelectBuilder]]. Entered via a `.where` call whose lambda returns a `TypedWhere[A]`; from that point on the
 * chain stays typed and `.compile` yields `CompiledQuery[Args, R]` with the concrete `Args` tuple surfaced.
 *
 * Operations that don't capture runtime values (`.orderBy`, `.limit`, `.offset`, `.distinctRows`, row-level
 * locking) pass `Args` through unchanged. A second typed `.where(typedPred)` extends `Args` to the twiddle pair
 * `(Args, A)` — same as `TypedWhere.&&`.
 */
final class TypedSelectBuilder[Ss <: Tuple, Args] private[dsl] (
  private[dsl] val base:          SelectBuilder[Ss],
  private[dsl] val whereFragment: Fragment[Args],
  private[dsl] val whereArgs:     Args,
  private[dsl] val orderBys:      List[OrderBy] = Nil,
  private[dsl] val limitOpt:      Option[Int] = None,
  private[dsl] val offsetOpt:     Option[Int] = None,
  private[dsl] val distinct:      Boolean = false
) {

  private def withState[A2](
    whereFragment: Fragment[A2] = this.whereFragment.asInstanceOf[Fragment[A2]],
    whereArgs:     A2           = this.whereArgs.asInstanceOf[A2],
    orderBys:      List[OrderBy] = this.orderBys,
    limitOpt:      Option[Int]   = this.limitOpt,
    offsetOpt:     Option[Int]   = this.offsetOpt,
    distinct:      Boolean       = this.distinct
  ): TypedSelectBuilder[Ss, A2] =
    new TypedSelectBuilder[Ss, A2](base, whereFragment, whereArgs, orderBys, limitOpt, offsetOpt, distinct)

  private def view: SelectView[Ss] = buildSelectView[Ss](base.sources)

  /** Chain another typed WHERE — AND-joined, with `Args` extended to `(Args, A)`. */
  def where[A](f: SelectView[Ss] => TypedWhere[A]): TypedSelectBuilder[Ss, (Args, A)] = {
    val pred = f(view)
    val combinedParts =
      RawConstants.OPEN_PAREN.fragment.parts ++
        whereFragment.parts ++
        RawConstants.AND.fragment.parts ++
        pred.fragment.parts ++
        RawConstants.CLOSE_PAREN.fragment.parts
    val combinedEncoder = whereFragment.encoder.product(pred.fragment.encoder)
    val combinedFragment: Fragment[(Args, A)] =
      Fragment(combinedParts, combinedEncoder, Origin.unknown)
    withState[(Args, A)](combinedFragment, (whereArgs, pred.args))
  }

  /** `ORDER BY …` — no runtime params captured (columns + direction keywords only). `Args` unchanged. */
  def orderBy(f: SelectView[Ss] => OrderBy | Tuple): TypedSelectBuilder[Ss, Args] = {
    val fresh = f(view) match {
      case ob: OrderBy => List(ob)
      case t: Tuple    => t.toList.asInstanceOf[List[OrderBy]]
    }
    withState[Args](orderBys = orderBys ++ fresh)
  }

  /** `LIMIT n` — `n` is rendered as a SQL integer literal, no `$N`. `Args` unchanged. */
  def limit(n: Int): TypedSelectBuilder[Ss, Args] = withState[Args](limitOpt = Some(n))

  /** `OFFSET n`. Same treatment as `limit`. */
  def offset(n: Int): TypedSelectBuilder[Ss, Args] = withState[Args](offsetOpt = Some(n))

  /** `SELECT DISTINCT`. */
  def distinctRows: TypedSelectBuilder[Ss, Args] = withState[Args](distinct = true)

  /**
   * Compile the typed chain into a `CompiledQuery[Args, R]`. Manual assembly — the typed Fragment[Args] must
   * drive the final encoder so `Args` isn't erased by the `AppliedFragment` `|+|` composition used for
   * untyped builds.
   */
  def compile(using ev: IsSingleSource[Ss]): CompiledQuery[Args, NamedRowOf[ev.Cols]] = {
    val entries = base.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head

    val selectKw: AppliedFragment =
      if (distinct) RawConstants.SELECT_DISTINCT else RawConstants.SELECT

    // SELECT [DISTINCT] * FROM <relation> WHERE <typed> [ORDER BY …] [LIMIT n] [OFFSET n]
    val projFrom: AppliedFragment =
      selectKw |+| TypedExpr.raw("*") |+| RawConstants.FROM |+| aliasedFromEntry(head)
    val wherePrefix: AppliedFragment = RawConstants.WHERE

    val afterOrderBy: List[Either[String, cats.data.State[Int, String]]] =
      if (orderBys.isEmpty) Nil
      else {
        val af = RawConstants.ORDER_BY |+| TypedExpr.joined(orderBys.map(_.af), ", ")
        af.fragment.parts
      }

    val afterLimit: List[Either[String, cats.data.State[Int, String]]] =
      limitOpt.fold(Nil: List[Either[String, cats.data.State[Int, String]]])(n =>
        TypedExpr.raw(s" LIMIT $n").fragment.parts
      )

    val afterOffset: List[Either[String, cats.data.State[Int, String]]] =
      offsetOpt.fold(Nil: List[Either[String, cats.data.State[Int, String]]])(n =>
        TypedExpr.raw(s" OFFSET $n").fragment.parts
      )

    val combinedParts: List[Either[String, cats.data.State[Int, String]]] =
      projFrom.fragment.parts ++
        wherePrefix.fragment.parts ++
        whereFragment.parts ++
        afterOrderBy ++
        afterLimit ++
        afterOffset

    val combinedFragment: Fragment[Args] =
      Fragment(combinedParts, whereFragment.encoder, Origin.unknown)

    val codec: Codec[NamedRowOf[ev.Cols]] =
      rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]

    CompiledQuery.mk[Args, NamedRowOf[ev.Cols]](combinedFragment, whereArgs, codec)
  }
}
