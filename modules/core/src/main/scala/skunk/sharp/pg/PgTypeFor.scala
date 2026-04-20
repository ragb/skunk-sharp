/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package skunk.sharp.pg

import skunk.Codec
import skunk.codec.all as pg

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
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
}

object PgTypeFor {

  def apply[T](using pf: PgTypeFor[T]): PgTypeFor[T] = pf

  def instance[T](c: Codec[T]): PgTypeFor[T] =
    new PgTypeFor[T] {
      val codec = c
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

  given [T](using p: PgTypeFor[T]): PgTypeFor[Option[T]] = instance(p.codec.opt)
}
