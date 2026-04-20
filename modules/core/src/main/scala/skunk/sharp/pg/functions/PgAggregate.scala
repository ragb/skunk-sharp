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
import skunk.sharp.pg.PgTypeFor

/**
 * Aggregate functions. Mixed into [[skunk.sharp.Pg]].
 *
 * Placement: all aggregates produce `TypedExpr[T]`, so they slot into SELECT projections, HAVING predicates, or ORDER
 * BY expressions. Correctness of `GROUP BY` coverage is not enforced at compile time — Postgres raises that as a loud
 * runtime error. See the builder's `.groupBy(...)` for explicit groups.
 */
trait PgAggregate {

  /** `count(*)` — row count including NULLs. */
  val countAll: TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(*)"), skunk.codec.all.int8)

  /** `count(expr)` — count of non-null values. */
  def count[T](expr: TypedExpr[T]): TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.int8)

  /** `count(DISTINCT expr)` — count of distinct non-null values. */
  def countDistinct[T](expr: TypedExpr[T]): TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(DISTINCT ") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.int8)

  /**
   * `sum(expr)` — result type follows Postgres's actual rules via [[SumOf]]:
   *   - `sum(smallint | integer)` → `bigint` (`Long`)
   *   - `sum(bigint | numeric)` → `numeric` (`BigDecimal`)
   *   - `sum(real)` → `real` (`Float`)
   *   - `sum(double precision)` → `double` (`Double`)
   */
  def sum[I](expr: TypedExpr[I])(using pf: PgTypeFor[SumOf[I]]): TypedExpr[SumOf[I]] =
    TypedExpr(TypedExpr.raw("sum(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /**
   * `avg(expr)` — integer / bigint / numeric → `numeric` (`BigDecimal`); `real` or `double` → `double` (`Double`).
   */
  def avg[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("avg(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `min(expr)` — same type as input. */
  def min[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("min(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /** `max(expr)` — same type as input. */
  def max[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("max(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /**
   * `string_agg(expr, sep)` — concatenate string values with a separator. Accepts any string-like tag on the aggregated
   * expression.
   */
  def stringAgg[T](expr: TypedExpr[T], sep: String)(using StrLike[T]): TypedExpr[String] =
    TypedExpr(
      TypedExpr.raw("string_agg(") |+| expr.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.lit(sep).render |+| TypedExpr.raw(")"),
      skunk.codec.all.text
    )

  /** `bool_and(expr)` — true if every non-null input is true. */
  def boolAnd(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_and(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

  /** `bool_or(expr)` — true if any non-null input is true. */
  def boolOr(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_or(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

}
