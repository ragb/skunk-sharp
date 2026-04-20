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

package skunk.sharp.circe

import io.circe.Json as CirceJson
import io.circe.{Decoder, Encoder}
import skunk.Codec
import skunk.circe.codec.all as circeCodecs
import skunk.sharp.pg.PgTypeFor

/**
 * Parameterised tag types mapping Postgres `json` / `jsonb` columns to Scala values via circe.
 *
 *   - `Jsonb[io.circe.Json]` / `Json[io.circe.Json]` — raw JSON, no schema. Pick this when the shape is dynamic or you
 *     want to traverse with the operators (`get`, `getText`, `path`, `contains`, …).
 *   - `Jsonb[A]` / `Json[A]` where `A` has both a circe `Encoder[A]` and `Decoder[A]` — typed round-trip. The column
 *     encodes from and decodes directly to `A`, same as skunk-circe's `jsonb[A]` / `json[A]` codecs.
 *
 * Example — mix typed and raw on the same row:
 *
 * {{{
 *   import io.circe.{Decoder, Encoder, Codec as CirceCodec}
 *
 *   case class Preferences(theme: String, notifications: Boolean) derives CirceCodec.AsObject
 *
 *   case class Document(
 *     id:       UUID,
 *     prefs:    Jsonb[Preferences],   // decodes straight into the case class
 *     metadata: Jsonb[io.circe.Json]  // raw, traverse with .get / .path / ...
 *   )
 *
 *   val docs = Table.of[Document]("documents")
 * }}}
 *
 * All operators (see [[JsonOps]]) accept any `TypedExpr[Jsonb[?]]` regardless of the type parameter — the operators act
 * at the SQL level on jsonb bytes. When a navigation step "loses" the static shape (e.g. `.get("someField")`), the
 * result widens to `Jsonb[io.circe.Json]`.
 */

/** Postgres `json` (text-preserving). Prefer [[Jsonb]] for indexed / operator-rich columns. */
opaque type Json[A] <: A = A

object Json {

  /** Construct a `Json[A]`-tagged value from an `A`. */
  def apply[A](a: A): Json[A] = a

  /** Unwrap to the underlying `A`. */
  extension [A](j: Json[A]) def unwrap: A = j

  /** Typed codec — works for any `A` with circe `Encoder` + `Decoder`, including `io.circe.Json` itself. */
  given [A](using enc: Encoder[A], dec: Decoder[A]): PgTypeFor[Json[A]] =
    PgTypeFor.instance(circeCodecs.json[A].asInstanceOf[Codec[Json[A]]])

}

/** Postgres `jsonb` (binary, indexable, operator-rich). Most operators in this module target this tag. */
opaque type Jsonb[A] <: A = A

/**
 * Namespace for `Jsonb[A]` — combines the tag companion (construct / unwrap / `PgTypeFor`) with the full [[PgJsonb]]
 * trait so `Jsonb.toJsonb`, `Jsonb.jsonbSet`, etc. are all available without extra imports.
 */
object Jsonb extends PgJsonb {

  /** Construct a `Jsonb[A]`-tagged value from an `A`. */
  def apply[A](a: A): Jsonb[A] = a

  /** Unwrap to the underlying `A`. */
  extension [A](j: Jsonb[A]) def unwrap: A = j

  /** Typed codec — works for any `A` with circe `Encoder` + `Decoder`, including `io.circe.Json`. */
  given [A](using enc: Encoder[A], dec: Decoder[A]): PgTypeFor[Jsonb[A]] =
    PgTypeFor.instance(circeCodecs.jsonb[A].asInstanceOf[Codec[Jsonb[A]]])

}

/**
 * Backwards-compatible aliases for the common raw-JSON case. `RawJson.Jsonb` / `RawJson.Json` read cleaner than the
 * full `Jsonb[io.circe.Json]` in case classes when the column really is schema-less.
 */
object RawJson {
  type Jsonb = skunk.sharp.circe.Jsonb[CirceJson]
  type Json  = skunk.sharp.circe.Json[CirceJson]
}
