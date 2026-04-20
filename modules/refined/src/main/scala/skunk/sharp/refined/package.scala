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

package skunk.sharp.refined

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.{MaxSize, Size}
import eu.timepit.refined.generic.Equal
import skunk.Codec
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.pg.tags.{Bpchar, Varchar}

/**
 * Integration with [refined](https://github.com/fthomas/refined).
 *
 * Parallel to the Iron module ([[skunk.sharp.iron]]) — same two-level support:
 *
 *   - The **fallback** `refinedPgTypeFor[A, C]` reuses the base `PgTypeFor[A]` — the constraint is Scala-only, the
 *     codec and Postgres type mirror the underlying unrefined type. This kicks in for constraints that have no DB
 *     counterpart (`Positive`, `MatchesRegex[…]`, `Greater[N]`, etc.).
 *   - **Bridges** from common refined constraints to skunk-sharp's tag types in core (see `skunk.sharp.pg.tags`).
 *     Example: `String Refined MaxSize[256]` picks the `varchar(256)` codec by routing to `PgTypeFor[Varchar[256]]`.
 *     Bridges are more specific than the fallback, so given-resolution prefers them automatically.
 *
 * Decoding does NOT re-validate the refined constraint — we trust what Postgres returns. Wrap with `Codec.emap` +
 * `refineV` if you want strict validation on decode.
 */
given refinedPgTypeFor[A, C](using base: PgTypeFor[A]): PgTypeFor[A Refined C] =
  PgTypeFor.instance(
    base.codec.imap[A Refined C](Refined.unsafeApply[A, C](_))(_.value)
  )

/** `String Refined MaxSize[N]` ⇒ `varchar(n)`. More specific than the fallback; implicit resolution prefers it. */
given varcharFromMaxSize[N <: Int](using pf: PgTypeFor[Varchar[N]]): PgTypeFor[String Refined MaxSize[N]] =
  PgTypeFor.instance(pf.codec.asInstanceOf[Codec[String Refined MaxSize[N]]])

/** `String Refined Size[Equal[N]]` ⇒ `bpchar(n)`. Mirrors Iron's `FixedLength[N]` bridge. */
given bpcharFromSizeEqual[N <: Int](using
  pf: PgTypeFor[Bpchar[N]]
)
  : PgTypeFor[String Refined Size[Equal[N]]] =
  PgTypeFor.instance(pf.codec.asInstanceOf[Codec[String Refined Size[Equal[N]]]])

object syntax {

  /**
   * Given a refined `Codec[A Refined C]`, downcast to a `Codec[A]` for interop with legacy code that needs the plain
   * base type.
   */
  extension [A, C](c: Codec[A Refined C]) def toBaseCodec: Codec[A] = c.asInstanceOf[Codec[A]]

}
