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

package skunk.sharp

import skunk.sharp.pg.functions.*

/**
 * Built-in Postgres functions. `object Pg` is a deliberate mixin of per-category traits from
 * [[skunk.sharp.pg.functions]] — numeric, string, time, aggregate, null-handling, and subquery — so the surface can
 * grow without any one file getting out of hand.
 *
 * Third-party extensions (jsonb, ltree, arrays, PostGIS, …) are expected to ship their own traits and a matching
 * namespace object, e.g.:
 *
 * {{{
 *   trait JsonbFns { def ->>(e: TypedExpr[Jsonb], key: String): TypedExpr[String] = … }
 *   object Jsonb extends JsonbFns
 * }}}
 *
 * or a custom `Pg`-like bundle mixing both:
 *
 * {{{
 *   object MyPg extends skunk.sharp.pg.functions.PgNumeric
 *     with skunk.sharp.pg.functions.PgString
 *     with my.extensions.JsonbFns
 * }}}
 */
object Pg
    extends PgNumeric
    with PgString
    with PgTime
    with PgAggregate
    with PgNull
    with PgSubquery
    with PgArray
