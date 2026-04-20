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

import skunk.codec.all as pg
import skunk.data.Arr
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Evidence that `A` is a Postgres-array-shaped Scala type. Currently only skunk's native `Arr[T]` has a built-in
 * instance; users who map `Arr` into their own collection type via `.imap` can ship their own `IsArray.Aux` given.
 *
 * Carries the element type as a type member so operators/functions can mention the element when needed (e.g.
 * `array_append(arr, elem)`) or just assert "A is array-ish" without mentioning the element (e.g. `array_length`).
 */
sealed trait IsArray[A] {
  type Elem
}

object IsArray {

  type Aux[A, E] = IsArray[A] { type Elem = E }

  given arrIsArray[T]: IsArray.Aux[Arr[T], T] = new IsArray[Arr[T]] { type Elem = T }

}

/**
 * Array operators as extension methods on `TypedExpr[A]` where `IsArray[A]` — works uniformly for `Arr[T]` and
 * `List[T]`-typed columns.
 *
 * Operator mapping:
 *   - `.contains(other)` → `a @> other`
 *   - `.containedBy(other)` → `a <@ other`
 *   - `.overlaps(other)` → `a && other`
 *   - `.concat(other)` → `a || other`
 *   - `elem.elemOf(a)` → `elem = ANY(a)`
 *
 * Postgres doesn't have a native `col IN array` form — `= ANY(…)` is the idiomatic alternative, surfaced as `.elemOf`.
 * Use `.in(NonEmptyList.of(…))` for classical `IN (literal-list)` / `IN (subquery)` via `skunk.sharp.where`.
 */
object ArrayOps {

  private def boolOp[A](op: String, l: TypedExpr[A], r: TypedExpr[A]): Where = {
    val af = l.render |+| TypedExpr.raw(s" $op ") |+| r.render
    Where(new TypedExpr[Boolean] {
      val render = af
      val codec  = pg.bool
    })
  }

  extension [A](lhs: TypedExpr[A])(using @annotation.unused ev: IsArray[A]) {

    /** `a @> b` — left array contains every element of right array. */
    def contains(rhs: TypedExpr[A]): Where = boolOp("@>", lhs, rhs)

    /** `a <@ b` — left array is contained by the right array. */
    def containedBy(rhs: TypedExpr[A]): Where = boolOp("<@", lhs, rhs)

    /** `a && b` — arrays share at least one element. */
    def overlaps(rhs: TypedExpr[A]): Where = boolOp("&&", lhs, rhs)

    /** `a || b` — concatenate arrays. Renders as `||`; result type is the same array type. */
    def concat(rhs: TypedExpr[A]): TypedExpr[A] =
      TypedExpr(lhs.render |+| TypedExpr.raw(" || ") |+| rhs.render, lhs.codec)

  }

  extension [E](elem: TypedExpr[E]) {

    /** `elem = ANY(array)` — does the array contain the given element? */
    def elemOf[A](arr: TypedExpr[A])(using @annotation.unused ev: IsArray.Aux[A, E]): Where = {
      val af = elem.render |+| TypedExpr.raw(" = ANY(") |+| arr.render |+| TypedExpr.raw(")")
      Where(new TypedExpr[Boolean] {
        val render = af
        val codec  = pg.bool
      })
    }

  }

}
