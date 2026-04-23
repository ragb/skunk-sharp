package skunk.sharp.pg.functions

import skunk.codec.all as pg
import skunk.sharp.TypedExpr
import skunk.sharp.pg.{IsRange, PgTypeFor}
import skunk.sharp.pg.tags.PgRange as PgRangeTag

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}

/**
 * Postgres range functions. Mixed into [[skunk.sharp.Pg]].
 *
 * Accessor functions:
 *   - `rangeLower(r)` → `lower(r)` — lower bound (NULL if unbounded)
 *   - `rangeUpper(r)` → `upper(r)` — upper bound (NULL if unbounded)
 *   - `rangeIsEmpty(r)` → `isempty(r)`
 *   - `rangeLowerInc(r)` → `lower_inc(r)` — true if lower bound is inclusive
 *   - `rangeUpperInc(r)` → `upper_inc(r)`
 *   - `rangeLowerInf(r)` → `lower_inf(r)` — true if lower bound is −∞
 *   - `rangeUpperInf(r)` → `upper_inf(r)`
 *
 * Constructor functions (two-argument form, uses default `[)` bounds):
 *   - `int4range(lo, hi)` / `int4range(lo, hi, bounds)`
 *   - `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange` — same pattern
 */
trait PgRangeFns {

  // -------- Accessors -----------------------------------------------------------------------

  /** `lower(r)` — lower bound of the range; NULL if the bound is unbounded (−∞). */
  def rangeLower[R, E](r: TypedExpr[R])(using
    @annotation.unused ev: IsRange.Aux[R, E],
    pf: PgTypeFor[Option[E]]
  ): TypedExpr[Option[E]] =
    TypedExpr(TypedExpr.raw("lower(") |+| r.render |+| TypedExpr.raw(")"), pf.codec)

  /** `upper(r)` — upper bound of the range; NULL if the bound is unbounded (+∞). */
  def rangeUpper[R, E](r: TypedExpr[R])(using
    @annotation.unused ev: IsRange.Aux[R, E],
    pf: PgTypeFor[Option[E]]
  ): TypedExpr[Option[E]] =
    TypedExpr(TypedExpr.raw("upper(") |+| r.render |+| TypedExpr.raw(")"), pf.codec)

  /** `isempty(r)` — true if the range contains no elements. */
  def rangeIsEmpty[R](r: TypedExpr[R])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("isempty(") |+| r.render |+| TypedExpr.raw(")"), pg.bool)

  /** `lower_inc(r)` — true if the lower bound is inclusive. */
  def rangeLowerInc[R](r: TypedExpr[R])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("lower_inc(") |+| r.render |+| TypedExpr.raw(")"), pg.bool)

  /** `upper_inc(r)` — true if the upper bound is inclusive. */
  def rangeUpperInc[R](r: TypedExpr[R])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("upper_inc(") |+| r.render |+| TypedExpr.raw(")"), pg.bool)

  /** `lower_inf(r)` — true if the lower bound is −∞ (unbounded). */
  def rangeLowerInf[R](r: TypedExpr[R])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("lower_inf(") |+| r.render |+| TypedExpr.raw(")"), pg.bool)

  /** `upper_inf(r)` — true if the upper bound is +∞ (unbounded). */
  def rangeUpperInf[R](r: TypedExpr[R])(using @annotation.unused ev: IsRange[R]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("upper_inf(") |+| r.render |+| TypedExpr.raw(")"), pg.bool)

  // -------- Constructor functions -----------------------------------------------------------

  /** `int4range(lo, hi)` — construct an `int4range` with default `[)` bounds. */
  def int4range(lo: TypedExpr[Int], hi: TypedExpr[Int])(using
    pf: PgTypeFor[PgRangeTag[Int]]
  ): TypedExpr[PgRangeTag[Int]] =
    TypedExpr(
      TypedExpr.raw("int4range(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `int4range(lo, hi, bounds)` — construct an `int4range` with explicit bounds string (e.g. `"[]"`, `"()"`, …). */
  def int4range(lo: TypedExpr[Int], hi: TypedExpr[Int], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[Int]]
  ): TypedExpr[PgRangeTag[Int]] =
    TypedExpr(
      TypedExpr.raw("int4range(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `int8range(lo, hi)` — construct an `int8range` with default `[)` bounds. */
  def int8range(lo: TypedExpr[Long], hi: TypedExpr[Long])(using
    pf: PgTypeFor[PgRangeTag[Long]]
  ): TypedExpr[PgRangeTag[Long]] =
    TypedExpr(
      TypedExpr.raw("int8range(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `int8range(lo, hi, bounds)`. */
  def int8range(lo: TypedExpr[Long], hi: TypedExpr[Long], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[Long]]
  ): TypedExpr[PgRangeTag[Long]] =
    TypedExpr(
      TypedExpr.raw("int8range(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `numrange(lo, hi)` — construct a `numrange` with default `[)` bounds. */
  def numrange(lo: TypedExpr[BigDecimal], hi: TypedExpr[BigDecimal])(using
    pf: PgTypeFor[PgRangeTag[BigDecimal]]
  ): TypedExpr[PgRangeTag[BigDecimal]] =
    TypedExpr(
      TypedExpr.raw("numrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `numrange(lo, hi, bounds)`. */
  def numrange(lo: TypedExpr[BigDecimal], hi: TypedExpr[BigDecimal], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[BigDecimal]]
  ): TypedExpr[PgRangeTag[BigDecimal]] =
    TypedExpr(
      TypedExpr.raw("numrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `daterange(lo, hi)` — construct a `daterange` with default `[)` bounds. */
  def daterange(lo: TypedExpr[LocalDate], hi: TypedExpr[LocalDate])(using
    pf: PgTypeFor[PgRangeTag[LocalDate]]
  ): TypedExpr[PgRangeTag[LocalDate]] =
    TypedExpr(
      TypedExpr.raw("daterange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `daterange(lo, hi, bounds)`. */
  def daterange(lo: TypedExpr[LocalDate], hi: TypedExpr[LocalDate], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[LocalDate]]
  ): TypedExpr[PgRangeTag[LocalDate]] =
    TypedExpr(
      TypedExpr.raw("daterange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `tsrange(lo, hi)` — construct a `tsrange` with default `[)` bounds. */
  def tsrange(lo: TypedExpr[LocalDateTime], hi: TypedExpr[LocalDateTime])(using
    pf: PgTypeFor[PgRangeTag[LocalDateTime]]
  ): TypedExpr[PgRangeTag[LocalDateTime]] =
    TypedExpr(
      TypedExpr.raw("tsrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `tsrange(lo, hi, bounds)`. */
  def tsrange(lo: TypedExpr[LocalDateTime], hi: TypedExpr[LocalDateTime], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[LocalDateTime]]
  ): TypedExpr[PgRangeTag[LocalDateTime]] =
    TypedExpr(
      TypedExpr.raw("tsrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `tstzrange(lo, hi)` — construct a `tstzrange` with default `[)` bounds. */
  def tstzrange(lo: TypedExpr[OffsetDateTime], hi: TypedExpr[OffsetDateTime])(using
    pf: PgTypeFor[PgRangeTag[OffsetDateTime]]
  ): TypedExpr[PgRangeTag[OffsetDateTime]] =
    TypedExpr(
      TypedExpr.raw("tstzrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `tstzrange(lo, hi, bounds)`. */
  def tstzrange(lo: TypedExpr[OffsetDateTime], hi: TypedExpr[OffsetDateTime], bounds: String)(using
    pf: PgTypeFor[PgRangeTag[OffsetDateTime]]
  ): TypedExpr[PgRangeTag[OffsetDateTime]] =
    TypedExpr(
      TypedExpr.raw("tstzrange(") |+| lo.render |+| TypedExpr.raw(", ") |+| hi.render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(bounds).render |+| TypedExpr.raw(")"),
      pf.codec
    )

}
