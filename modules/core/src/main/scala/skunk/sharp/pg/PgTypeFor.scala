/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package skunk.sharp.pg

import skunk.Codec
import skunk.codec.all as pg

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

/** Type class pairing a Scala type with its default [[PgType]] and skunk [[Codec]].
  *
  * Extension point: user modules ship `given PgTypeFor[MyType]` instances to teach the core (and the schema validator)
  * about their Postgres types. The built-in instances here cover the common base scalars; see `skunk-sharp-iron` for an
  * example of adding refinement-aware instances.
  */
trait PgTypeFor[T]:
  def pgType: PgType
  def codec: Codec[T]

object PgTypeFor:

  def apply[T](using pf: PgTypeFor[T]): PgTypeFor[T] = pf

  def instance[T](t: PgType, c: Codec[T]): PgTypeFor[T] =
    new PgTypeFor[T]:
      val pgType = t
      val codec  = c

  given PgTypeFor[Boolean]        = instance(PgType.Bool, pg.bool)
  given PgTypeFor[Short]          = instance(PgType.Int2, pg.int2)
  given PgTypeFor[Int]            = instance(PgType.Int4, pg.int4)
  given PgTypeFor[Long]           = instance(PgType.Int8, pg.int8)
  given PgTypeFor[Float]          = instance(PgType.Float4, pg.float4)
  given PgTypeFor[Double]         = instance(PgType.Float8, pg.float8)
  given PgTypeFor[BigDecimal]     = instance(PgType.Numeric, pg.numeric)
  given PgTypeFor[String]         = instance(PgType.Text, pg.text)
  given PgTypeFor[UUID]           = instance(PgType.Uuid, pg.uuid)
  given PgTypeFor[LocalDate]      = instance(PgType.Date, pg.date)
  given PgTypeFor[LocalTime]      = instance(PgType.Time, pg.time)
  given PgTypeFor[OffsetTime]     = instance(PgType.Timetz, pg.timetz)
  given PgTypeFor[LocalDateTime]  = instance(PgType.Timestamp, pg.timestamp)
  given PgTypeFor[OffsetDateTime] = instance(PgType.Timestamptz, pg.timestamptz)

  /** Option lifts preserve the underlying `pgType` — nullability is tracked separately on the column itself, not on
    * `PgType`.
    */
  given [T](using p: PgTypeFor[T]): PgTypeFor[Option[T]] =
    instance(p.pgType, p.codec.opt)
