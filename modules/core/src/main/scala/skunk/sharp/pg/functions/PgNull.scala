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

package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Stripped

/** NULL-handling helpers. Mixed into [[skunk.sharp.Pg]]. */
trait PgNull {

  /** `coalesce(a, b, c, …)` — first non-null argument. */
  def coalesce[T](args: TypedExpr[T]*)(using pfr: PgTypeFor[T]): TypedExpr[T] =
    PgFunction.nary[T]("coalesce", args*)

  /**
   * `nullif(a, b)` — returns NULL if `a = b`, else `a`. Result is always nullable because the caller can't prove
   * `a ≠ b` statically. `b` must have the same underlying type as `a` (modulo `Option` wrapping).
   */
  def nullif[T](a: TypedExpr[T], b: Stripped[T])(using
    pf: PgTypeFor[Stripped[T]]
  ): TypedExpr[Option[Stripped[T]]] =
    TypedExpr(
      TypedExpr.raw("nullif(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(b).render |+| TypedExpr.raw(")"),
      pf.codec.opt
    )

}
