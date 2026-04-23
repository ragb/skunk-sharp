package skunk.sharp.pg

import skunk.codec.all as pg
import skunk.sharp.data.Range

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
import scala.compiletime.constValue

/**
 * Opaque type aliases that tag a Scala base type with its exact Postgres storage type, so codec selection (`PgTypeFor`)
 * has no ambiguity at `Table.of[T]` or `Table.builder.column[T]` derivation time.
 *
 * Tags are subtype aliases (`opaque type X <: Base = Base`), so a `Varchar[256]` is-a `String` — you pass it around,
 * print it, concatenate it like any `String`. The distinction exists only for typeclass resolution: a field of type
 * `Varchar[256]` summons a `varchar(256)` codec, a field of type `Int2` summons `int2`, and so on. Nothing new at
 * runtime: tags erase to their base type.
 *
 * When to use a tag:
 *   - your column is `varchar(n)` / `bpchar(n)` / `int2` / `numeric(p,s)` / `citext` / … — anything the default
 *     `PgTypeFor[String]` / `PgTypeFor[Int]` / … would pick wrong.
 *   - you want self-documenting case class fields: `email: Varchar[256]` states the storage precisely.
 *
 * Extensibility: additional tags live in companion modules (`skunk-sharp-json`, `skunk-sharp-ltree`, …) — each ships
 * its own `opaque type` plus a `given PgTypeFor[…]`. No changes in core required.
 *
 * Construction: use the tag's `apply` — `Varchar[256]("hello@x.com")`, `Int2(42.toShort)`. We deliberately do not ship
 * `given Conversion[Base, Tag]` because Scala's implicit conversions require an opt-in language import
 * (`scala.language.implicitConversions`) and can fire in unexpected places; users who want them can define their own.
 */
object tags {

  // -------- String family --------

  /** Variable-length string, `varchar(n)`. */
  opaque type Varchar[N <: Int] <: String = String

  object Varchar {
    inline def apply[N <: Int](s: String): Varchar[N] = s

    inline given [N <: Int]: PgTypeFor[Varchar[N]] =
      PgTypeFor.instance(pg.varchar(constValue[N]).asInstanceOf[skunk.Codec[Varchar[N]]])

  }

  /** Fixed-length blank-padded char, `char(n)` / `bpchar(n)`. */
  opaque type Bpchar[N <: Int] <: String = String

  object Bpchar {
    inline def apply[N <: Int](s: String): Bpchar[N] = s

    inline given [N <: Int]: PgTypeFor[Bpchar[N]] =
      PgTypeFor.instance(pg.bpchar(constValue[N]).asInstanceOf[skunk.Codec[Bpchar[N]]])

  }

  /** `text`. Same as the default `PgTypeFor[String]`; use this tag when you want the mapping spelled out. */
  opaque type Text <: String = String

  object Text {
    def apply(s: String): Text = s

    given PgTypeFor[Text] = PgTypeFor.instance(pg.text.asInstanceOf[skunk.Codec[Text]])
  }

  // -------- Integer family --------

  /** `int2` / `smallint`. */
  opaque type Int2 <: Short = Short

  object Int2 {
    def apply(v: Short): Int2 = v

    given PgTypeFor[Int2] = PgTypeFor.instance(pg.int2.asInstanceOf[skunk.Codec[Int2]])
  }

  /** `int4` / `integer`. Same as default `PgTypeFor[Int]`; use when you want the mapping spelled out. */
  opaque type Int4 <: Int = Int

  object Int4 {
    def apply(v: Int): Int4 = v

    given PgTypeFor[Int4] = PgTypeFor.instance(pg.int4.asInstanceOf[skunk.Codec[Int4]])
  }

  /** `int8` / `bigint`. Same as default `PgTypeFor[Long]`. */
  opaque type Int8 <: Long = Long

  object Int8 {
    def apply(v: Long): Int8 = v

    given PgTypeFor[Int8] = PgTypeFor.instance(pg.int8.asInstanceOf[skunk.Codec[Int8]])
  }

  // -------- Numeric with precision / scale --------

  /** `numeric(precision, scale)`. */
  opaque type Numeric[P <: Int, S <: Int] <: BigDecimal = BigDecimal

  object Numeric {
    inline def apply[P <: Int, S <: Int](v: BigDecimal): Numeric[P, S] = v

    inline given [P <: Int, S <: Int]: PgTypeFor[Numeric[P, S]] =
      PgTypeFor.instance(
        pg.numeric(constValue[P], constValue[S]).asInstanceOf[skunk.Codec[Numeric[P, S]]]
      )

  }

  // -------- Range family --------

  /**
   * Postgres range type parameterised by its element type. Use the concrete type aliases below (`Int4Range`,
   * `DateRange`, …) in case class fields; the opaque tag drives codec selection so `Table.of[T]` picks the right
   * `int4range` / `daterange` / … wire type.
   *
   * Construct values with `PgRange(lower = Some(1), upper = Some(10))` or `PgRange.empty`.
   */
  opaque type PgRange[A] <: Range[A] = Range[A]

  object PgRange {

    // ---- Concrete type aliases ----

    /** `int4range` — range of 32-bit integers. */
    type Int4Range = PgRange[Int]

    /** `int8range` — range of 64-bit integers. */
    type Int8Range = PgRange[Long]

    /** `numrange` — range of arbitrary-precision decimals. */
    type NumRange = PgRange[BigDecimal]

    /** `daterange` — range of calendar dates. */
    type DateRange = PgRange[LocalDate]

    /** `tsrange` — range of timestamps without time zone. */
    type TsRange = PgRange[LocalDateTime]

    /** `tstzrange` — range of timestamps with time zone. */
    type TstzRange = PgRange[OffsetDateTime]

    // ---- Constructors ----

    /** Wrap a `Range[A]` as a tagged `PgRange[A]`. */
    def apply[A](r: Range[A]): PgRange[A] = r

    /** Convenience constructor for a `Bounds` range. Defaults: `[lower, upper)`. */
    def apply[A](
      lower: Option[A] = None,
      upper: Option[A] = None,
      lowerInclusive: Boolean = true,
      upperInclusive: Boolean = false
    ): PgRange[A] = Range.Bounds(lower, upper, lowerInclusive, upperInclusive)

    /** The canonical empty range. */
    def empty[A]: PgRange[A] = Range.Empty

    // ---- PgTypeFor instances (explicit names avoid the auto-name clash that all `PgTypeFor[PgRange[?]]` would share) ----

    given int4RangePgTypeFor: PgTypeFor[PgRange[Int]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(pg.int4, skunk.data.Type("int4range")).asInstanceOf[skunk.Codec[PgRange[Int]]]
      )

    given int8RangePgTypeFor: PgTypeFor[PgRange[Long]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(pg.int8, skunk.data.Type("int8range")).asInstanceOf[skunk.Codec[PgRange[Long]]]
      )

    given numRangePgTypeFor: PgTypeFor[PgRange[BigDecimal]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(pg.numeric, skunk.data.Type("numrange")).asInstanceOf[skunk.Codec[PgRange[BigDecimal]]]
      )

    given dateRangePgTypeFor: PgTypeFor[PgRange[LocalDate]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(pg.date, skunk.data.Type("daterange")).asInstanceOf[skunk.Codec[PgRange[LocalDate]]]
      )

    given tsRangePgTypeFor: PgTypeFor[PgRange[LocalDateTime]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(
          pg.timestamp,
          skunk.data.Type("tsrange")
        ).asInstanceOf[skunk.Codec[PgRange[LocalDateTime]]]
      )

    given tstzRangePgTypeFor: PgTypeFor[PgRange[OffsetDateTime]] =
      PgTypeFor.instance(
        RangeCodecs.rangeCodec(
          pg.timestamptz,
          skunk.data.Type("tstzrange")
        ).asInstanceOf[skunk.Codec[PgRange[OffsetDateTime]]]
      )

  }

}
