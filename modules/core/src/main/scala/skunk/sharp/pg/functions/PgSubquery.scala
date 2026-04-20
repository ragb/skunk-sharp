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

import skunk.sharp.TypedExpr

/** Subquery-based predicates. Mixed into [[skunk.sharp.Pg]]. */
trait PgSubquery {

  /**
   * `EXISTS (<subquery>)`. The subquery's row type doesn't matter — only existence. Accepts any
   * [[skunk.sharp.dsl.AsSubquery]]-compatible value (compiled query or un-compiled builder).
   */
  def exists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

  /** `NOT EXISTS (<subquery>)`. */
  def notExists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("NOT EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

}
