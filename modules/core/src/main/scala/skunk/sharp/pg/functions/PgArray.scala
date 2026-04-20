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

import skunk.codec.all as pg
import skunk.data.Arr
import skunk.sharp.TypedExpr
import skunk.sharp.pg.{IsArray, PgTypeFor}

/**
 * Built-in Postgres array functions — `array_length`, `cardinality`, `array_append`, `array_prepend`, `array_cat`,
 * `array_position`, `array_positions`, `array_remove`, `array_replace`, `array_to_string`, `string_to_array`, `unnest`,
 * plus the `array_agg` aggregate.
 *
 * Operators (`@>`, `<@`, `&&`, `||`, `= ANY`) live as extension methods in [[skunk.sharp.pg.ArrayOps]].
 *
 * All functions accept any array-shaped Scala type via `IsArray[A]` — works uniformly for `Arr[T]` and `List[T]`
 * columns.
 */
trait PgArray {

  /** `array_length(a, dim)` → `Option[Int]`. Dimension is 1 for most Postgres arrays. Returns NULL for empty arrays. */
  def arrayLength[A](a: TypedExpr[A], dim: Int = 1)(using @annotation.unused ev: IsArray[A]): TypedExpr[Option[Int]] =
    TypedExpr(
      TypedExpr.raw("array_length(") |+| a.render |+| TypedExpr.raw(s", $dim)"),
      pg.int4.opt
    )

  /** `cardinality(a)` → `Int` — total element count across all dimensions. */
  def cardinality[A](a: TypedExpr[A])(using @annotation.unused ev: IsArray[A]): TypedExpr[Int] =
    TypedExpr(TypedExpr.raw("cardinality(") |+| a.render |+| TypedExpr.raw(")"), pg.int4)

  /** `array_append(a, elem)`. Result keeps the same array Scala type as the input. */
  def arrayAppend[A, E](a: TypedExpr[A], elem: TypedExpr[E])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A] =
    TypedExpr(
      TypedExpr.raw("array_append(") |+| a.render |+| TypedExpr.raw(", ") |+| elem.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `array_prepend(elem, a)`. */
  def arrayPrepend[A, E](elem: TypedExpr[E], a: TypedExpr[A])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A] =
    TypedExpr(
      TypedExpr.raw("array_prepend(") |+| elem.render |+| TypedExpr.raw(", ") |+| a.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `array_cat(a, b)` — concatenate two arrays of the same type. */
  def arrayCat[A](a: TypedExpr[A], b: TypedExpr[A])(using @annotation.unused ev: IsArray[A]): TypedExpr[A] =
    TypedExpr(
      TypedExpr.raw("array_cat(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `array_position(a, elem)` → `Option[Int]` — 1-based index of the first match, NULL if absent. */
  def arrayPosition[A, E](a: TypedExpr[A], elem: TypedExpr[E])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Option[Int]] =
    TypedExpr(
      TypedExpr.raw("array_position(") |+| a.render |+| TypedExpr.raw(", ") |+| elem.render |+| TypedExpr.raw(")"),
      pg.int4.opt
    )

  /** `array_positions(a, elem)` → `Arr[Int]` — all 1-based indices of `elem` in `a`. */
  def arrayPositions[A, E](a: TypedExpr[A], elem: TypedExpr[E])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Arr[Int]] =
    TypedExpr(
      TypedExpr.raw("array_positions(") |+| a.render |+| TypedExpr.raw(", ") |+| elem.render |+| TypedExpr.raw(")"),
      pg._int4
    )

  /** `array_remove(a, elem)` — all occurrences of `elem` dropped. */
  def arrayRemove[A, E](a: TypedExpr[A], elem: TypedExpr[E])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A] =
    TypedExpr(
      TypedExpr.raw("array_remove(") |+| a.render |+| TypedExpr.raw(", ") |+| elem.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `array_replace(a, from, to)` — replace every `from` with `to`. */
  def arrayReplace[A, E](a: TypedExpr[A], from: TypedExpr[E], to: TypedExpr[E])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A] =
    TypedExpr(
      TypedExpr.raw("array_replace(") |+| a.render |+| TypedExpr.raw(", ") |+| from.render |+|
        TypedExpr.raw(", ") |+| to.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /**
   * `array_to_string(a, sep)` → `String`. NULL elements are skipped.
   */
  def arrayToString[A](a: TypedExpr[A], sep: String)(using @annotation.unused ev: IsArray[A]): TypedExpr[String] =
    TypedExpr(
      TypedExpr.raw("array_to_string(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(sep).render |+|
        TypedExpr.raw(")"),
      pg.text
    )

  /** `array_to_string(a, sep, nullStr)` → `String`. NULL elements are replaced with `nullStr`. */
  def arrayToString[A](a: TypedExpr[A], sep: String, nullStr: String)(using
    @annotation.unused ev: IsArray[A]
  ): TypedExpr[String] =
    TypedExpr(
      TypedExpr.raw("array_to_string(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(sep).render |+|
        TypedExpr.raw(", ") |+| TypedExpr.lit(nullStr).render |+| TypedExpr.raw(")"),
      pg.text
    )

  /** `string_to_array(s, sep)` → `Arr[String]`. Empty string separator splits by character. */
  def stringToArray(s: TypedExpr[String], sep: String): TypedExpr[Arr[String]] =
    TypedExpr(
      TypedExpr.raw("string_to_array(") |+| s.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(sep).render |+|
        TypedExpr.raw(")"),
      pg._text
    )

  /**
   * `array_agg(expr)` — aggregate rows into a single array. Returns `Arr[T]`; callers who want a Scala `List[T]` can
   * `.imap` or use the `List[T]` codec directly.
   */
  def arrayAgg[T](expr: TypedExpr[T])(using pf: PgTypeFor[Arr[T]]): TypedExpr[Arr[T]] =
    TypedExpr(TypedExpr.raw("array_agg(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /**
   * `unnest(a)` — set-returning function. Only meaningful in a FROM / LATERAL / SELECT-list context; renders the SQL
   * but the DSL doesn't yet have a set-returning-source abstraction, so this is primarily here for use in raw fragments
   * or subqueries.
   */
  def unnest[A, E](a: TypedExpr[A])(using
    @annotation.unused ev: IsArray.Aux[A, E],
    pf: PgTypeFor[E]
  ): TypedExpr[E] =
    TypedExpr(TypedExpr.raw("unnest(") |+| a.render |+| TypedExpr.raw(")"), pf.codec)

}
