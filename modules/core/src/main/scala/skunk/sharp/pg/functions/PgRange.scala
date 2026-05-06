package skunk.sharp.pg.functions


import skunk.{Fragment, Void}
import skunk.codec.all as pg
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.pg.{IsRange, PgTypeFor}
import skunk.sharp.pg.tags.PgRange as PgRangeTag
import skunk.sharp.where.Where

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}

/** Postgres range functions. Mixed into [[skunk.sharp.Pg]]. Args of input expression(s) propagate. */
trait PgRangeFns {

  // -------- Accessors -----------------------------------------------------------------------

  def rangeLower[R, E, X](r: TypedExpr[R, X])(using
    @annotation.unused ev: IsRange.Aux[R, E], pf: PgTypeFor[Option[E]]
  ): TypedExpr[Option[E], X] =
    unaryRange("lower", r, pf.codec)

  def rangeUpper[R, E, X](r: TypedExpr[R, X])(using
    @annotation.unused ev: IsRange.Aux[R, E], pf: PgTypeFor[Option[E]]
  ): TypedExpr[Option[E], X] =
    unaryRange("upper", r, pf.codec)

  def rangeIsEmpty[R, X](r: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean, X] =
    unaryRange("isempty", r, pg.bool)

  def rangeLowerInc[R, X](r: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean, X] =
    unaryRange("lower_inc", r, pg.bool)

  def rangeUpperInc[R, X](r: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean, X] =
    unaryRange("upper_inc", r, pg.bool)

  def rangeLowerInf[R, X](r: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean, X] =
    unaryRange("lower_inf", r, pg.bool)

  def rangeUpperInf[R, X](r: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean, X] =
    unaryRange("upper_inf", r, pg.bool)

  // -------- Two-arg range constructors -----------------------------------------------------------

  inline def int4range[X, Y](lo: TypedExpr[Int, X], hi: TypedExpr[Int, Y])(using
    pf: PgTypeFor[PgRangeTag[Int]]
  ): TypedExpr[PgRangeTag[Int], Where.Concat[X, Y]] = rangeCtor2("int4range", lo, hi, pf.codec)

  inline def int4range[X, Y](lo: TypedExpr[Int, X], hi: TypedExpr[Int, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[Int]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[Int], Where.Concat[X, Y]] = rangeCtor3("int4range", lo, hi, bounds, pf.codec)

  inline def int8range[X, Y](lo: TypedExpr[Long, X], hi: TypedExpr[Long, Y])(using
    pf: PgTypeFor[PgRangeTag[Long]]
  ): TypedExpr[PgRangeTag[Long], Where.Concat[X, Y]] = rangeCtor2("int8range", lo, hi, pf.codec)

  inline def int8range[X, Y](lo: TypedExpr[Long, X], hi: TypedExpr[Long, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[Long]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[Long], Where.Concat[X, Y]] = rangeCtor3("int8range", lo, hi, bounds, pf.codec)

  inline def numrange[X, Y](lo: TypedExpr[BigDecimal, X], hi: TypedExpr[BigDecimal, Y])(using
    pf: PgTypeFor[PgRangeTag[BigDecimal]]
  ): TypedExpr[PgRangeTag[BigDecimal], Where.Concat[X, Y]] = rangeCtor2("numrange", lo, hi, pf.codec)

  inline def numrange[X, Y](lo: TypedExpr[BigDecimal, X], hi: TypedExpr[BigDecimal, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[BigDecimal]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[BigDecimal], Where.Concat[X, Y]] = rangeCtor3("numrange", lo, hi, bounds, pf.codec)

  inline def daterange[X, Y](lo: TypedExpr[LocalDate, X], hi: TypedExpr[LocalDate, Y])(using
    pf: PgTypeFor[PgRangeTag[LocalDate]]
  ): TypedExpr[PgRangeTag[LocalDate], Where.Concat[X, Y]] = rangeCtor2("daterange", lo, hi, pf.codec)

  inline def daterange[X, Y](lo: TypedExpr[LocalDate, X], hi: TypedExpr[LocalDate, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[LocalDate]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[LocalDate], Where.Concat[X, Y]] = rangeCtor3("daterange", lo, hi, bounds, pf.codec)

  inline def tsrange[X, Y](lo: TypedExpr[LocalDateTime, X], hi: TypedExpr[LocalDateTime, Y])(using
    pf: PgTypeFor[PgRangeTag[LocalDateTime]]
  ): TypedExpr[PgRangeTag[LocalDateTime], Where.Concat[X, Y]] = rangeCtor2("tsrange", lo, hi, pf.codec)

  inline def tsrange[X, Y](lo: TypedExpr[LocalDateTime, X], hi: TypedExpr[LocalDateTime, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[LocalDateTime]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[LocalDateTime], Where.Concat[X, Y]] = rangeCtor3("tsrange", lo, hi, bounds, pf.codec)

  inline def tstzrange[X, Y](lo: TypedExpr[OffsetDateTime, X], hi: TypedExpr[OffsetDateTime, Y])(using
    pf: PgTypeFor[PgRangeTag[OffsetDateTime]]
  ): TypedExpr[PgRangeTag[OffsetDateTime], Where.Concat[X, Y]] = rangeCtor2("tstzrange", lo, hi, pf.codec)

  inline def tstzrange[X, Y](lo: TypedExpr[OffsetDateTime, X], hi: TypedExpr[OffsetDateTime, Y], bounds: String)(using pf: PgTypeFor[PgRangeTag[OffsetDateTime]], pfs: PgTypeFor[String]): TypedExpr[PgRangeTag[OffsetDateTime], Where.Concat[X, Y]] = rangeCtor3("tstzrange", lo, hi, bounds, pf.codec)

  // -------- Helpers -------------------------------------------------------------------------

  private def unaryRange[R, X, T](
    name: String, r: TypedExpr[R, X], outCodec: skunk.Codec[T]
  ): TypedExpr[T, X] = {
    val frag = TypedExpr.wrap(s"$name(", r.fragment, ")")
    TypedExpr[T, X](frag, outCodec)
  }

  private inline def rangeCtor2[T, A, B, X, Y](
    name: String, lo: TypedExpr[A, X], hi: TypedExpr[B, Y], outCodec: skunk.Codec[T]
  ): TypedExpr[T, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](lo.fragment, ", ", hi.fragment)
    val frag  = TypedExpr.wrap(s"$name(", inner, ")")
    TypedExpr[T, Where.Concat[X, Y]](frag, outCodec)
  }

  /**
   * Three-arg range constructor (lo, hi, bounds). `bounds` is a baked runtime String
   * (Param.bind); Args is `Concat[X, Y]` from `lo` / `hi`.
   */
  private inline def rangeCtor3[T, A, B, X, Y](
    name: String, lo: TypedExpr[A, X], hi: TypedExpr[B, Y], bounds: String, outCodec: skunk.Codec[T]
  )(using pfs: PgTypeFor[String]): TypedExpr[T, Where.Concat[X, Y]] = {
    val boundsFrag = Param.bind[String](bounds).fragment
    val parts =
      List[Either[String, cats.data.State[Int, String]]](Left(s"$name(")) ++
        lo.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(", ")) ++
        hi.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(", ")) ++
        boundsFrag.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    // lo/hi typed; bounds Void. Combine [Concat[X, Y], Void] with right-Void projection.
    val loHiEnc      = TypedExpr.combineEnc[X, Y](
      lo.fragment.encoder, hi.fragment.encoder, c => Where.projectConcat[X, Y](c)
    )
    val withBoundsEnc =
      TypedExpr.combineEnc[Where.Concat[X, Y], Void](
        loHiEnc.asInstanceOf[skunk.Encoder[Where.Concat[X, Y]]],
        boundsFrag.encoder,
        c => (c.asInstanceOf[Where.Concat[X, Y]], Void)
      )
    val frag = Fragment(parts, withBoundsEnc.asInstanceOf[skunk.Encoder[Where.Concat[X, Y]]], skunk.util.Origin.unknown)
    TypedExpr[T, Where.Concat[X, Y]](frag, outCodec)
  }

}
