package skunk.sharp.pg

import skunk.Codec
import skunk.codec.all as pg

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

/**
 * Default codec for a Scala type, used by the `Table.of[T]` / `View.of[T]` quick-start paths and by WHERE operators
 * that need to encode literal values (`col === 42`).
 *
 * This is *just* a wrapper around a skunk [[Codec]] — the Postgres type comes from the codec itself (`codec.types`).
 * Downstream code reads `skunk.data.Type` via `PgTypes.typeOf(codec)`.
 *
 * The built-in instances cover common scalars with the "most common" mapping (String → text, Int → int4, …). When the
 * actual column has a different type, switch to the codec-first builder (`.column("n", pg.varchar(256))`) or use
 * `table.withColumnCodec(...)`.
 */
trait PgTypeFor[T] {
  def codec: Codec[T]

  /**
   * Postgres extension name (e.g. `"citext"`, `"ltree"`, `"hstore"`) that must be installed on the database for this
   * type to function. `None` for built-in scalars and for any tag whose codec uses a built-in wire type. Contrib types
   * (see `skunk.sharp.contrib.*`) override this so the schema validator can flag missing extensions.
   */
  def requiredExtension: Option[String] = None
}

object PgTypeFor {

  def apply[T](using pf: PgTypeFor[T]): PgTypeFor[T] = pf

  def instance[T](c: Codec[T]): PgTypeFor[T] =
    new PgTypeFor[T] {
      val codec = c
    }

  /** Same as [[instance]] but tags this `PgTypeFor` as requiring the named Postgres extension. */
  def instanceWithExtension[T](c: Codec[T], extension: String): PgTypeFor[T] = {
    val ext = extension
    new PgTypeFor[T] {
      val codec                                  = c
      override val requiredExtension: Option[String] = Some(ext)
    }
  }

  given PgTypeFor[Boolean]        = instance(pg.bool)
  given PgTypeFor[Short]          = instance(pg.int2)
  given PgTypeFor[Int]            = instance(pg.int4)
  given PgTypeFor[Long]           = instance(pg.int8)
  given PgTypeFor[Float]          = instance(pg.float4)
  given PgTypeFor[Double]         = instance(pg.float8)
  given PgTypeFor[BigDecimal]     = instance(pg.numeric)
  given PgTypeFor[String]         = instance(pg.text)
  given PgTypeFor[UUID]           = instance(pg.uuid)
  given PgTypeFor[LocalDate]      = instance(pg.date)
  given PgTypeFor[LocalTime]      = instance(pg.time)
  given PgTypeFor[OffsetTime]     = instance(pg.timetz)
  given PgTypeFor[LocalDateTime]  = instance(pg.timestamp)
  given PgTypeFor[OffsetDateTime] = instance(pg.timestamptz)
  given PgTypeFor[Duration]       = instance(pg.interval)

  given [T](using p: PgTypeFor[T]): PgTypeFor[Option[T]] = {
    val inner = p
    new PgTypeFor[Option[T]] {
      val codec                                      = inner.codec.opt
      override val requiredExtension: Option[String] = inner.requiredExtension
    }
  }
}
