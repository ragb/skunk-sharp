package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor

/**
 * Window-only functions — ranking, distribution, offset access, and value functions that must be used with an
 * `OVER (…)` clause. Mixed into [[skunk.sharp.Pg]].
 *
 * Unlike aggregates, these functions have no meaning without a window frame — Postgres rejects them outside `OVER`.
 * The DSL doesn't enforce this at compile time (the `.over(…)` extension is always available on any `TypedExpr`), but
 * the names and semantics match the Postgres docs.
 *
 * Use the `.over(spec)` extension (imported via `skunk.sharp.dsl.*`) on any of these values / results:
 *
 * {{{
 *   import skunk.sharp.dsl.*
 *
 *   users.select(u => (
 *     u.email,
 *     Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc)),
 *     Pg.sum(u.age).over(WindowSpec.orderBy(u.age.asc).rowsBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow)),
 *     Pg.lag(u.age).over(WindowSpec.orderBy(u.age.asc))
 *   ))
 * }}}
 */
trait PgWindow {

  // ---- Ranking functions -----------------------------------------------------------------------

  /** `row_number()` — sequential number of the current row within its partition, counting from 1. */
  val rowNumber: TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("row_number()"), skunk.codec.all.int8)

  /** `rank()` — rank of the current row, with gaps for tied rows. */
  val rank: TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("rank()"), skunk.codec.all.int8)

  /** `dense_rank()` — rank without gaps. */
  val denseRank: TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("dense_rank()"), skunk.codec.all.int8)

  /** `percent_rank()` — relative rank as a fraction in `[0, 1]`. */
  val percentRank: TypedExpr[Double] =
    TypedExpr(TypedExpr.raw("percent_rank()"), skunk.codec.all.float8)

  /** `cume_dist()` — cumulative distribution: fraction of rows ≤ the current row. */
  val cumeDist: TypedExpr[Double] =
    TypedExpr(TypedExpr.raw("cume_dist()"), skunk.codec.all.float8)

  /** `ntile(n)` — partition rows into `n` buckets numbered 1..n. */
  def ntile(n: Int): TypedExpr[Int] =
    TypedExpr(TypedExpr.raw(s"ntile($n)"), skunk.codec.all.int4)

  // ---- Offset access functions -----------------------------------------------------------------

  /**
   * `lag(expr)` — value from the previous row within the partition; `None` (SQL `NULL`) if there is no previous row.
   */
  def lag[T](expr: TypedExpr[T]): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("lag(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec.opt)

  /**
   * `lag(expr, offset)` — value `offset` rows before the current row; `None` if out of partition.
   */
  def lag[T](expr: TypedExpr[T], offset: Int): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("lag(") |+| expr.render |+| TypedExpr.raw(s", $offset)"), expr.codec.opt)

  /**
   * `lag(expr, offset, default)` — value `offset` rows before; returns `default` instead of `NULL` when out of
   * partition. Result type is non-optional because the default fills the gap.
   */
  def lag[T](expr: TypedExpr[T], offset: Int, default: T)(using pf: PgTypeFor[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("lag(") |+| expr.render |+| TypedExpr.raw(s", $offset, ") |+|
        TypedExpr.parameterised(default).render |+| TypedExpr.raw(")"),
      expr.codec
    )

  /**
   * `lead(expr)` — value from the next row; `None` if there is no next row.
   */
  def lead[T](expr: TypedExpr[T]): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("lead(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec.opt)

  /**
   * `lead(expr, offset)` — value `offset` rows after the current row; `None` if out of partition.
   */
  def lead[T](expr: TypedExpr[T], offset: Int): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("lead(") |+| expr.render |+| TypedExpr.raw(s", $offset)"), expr.codec.opt)

  /**
   * `lead(expr, offset, default)` — value `offset` rows after; returns `default` instead of `NULL`.
   */
  def lead[T](expr: TypedExpr[T], offset: Int, default: T)(using pf: PgTypeFor[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("lead(") |+| expr.render |+| TypedExpr.raw(s", $offset, ") |+|
        TypedExpr.parameterised(default).render |+| TypedExpr.raw(")"),
      expr.codec
    )

  // ---- Value functions -------------------------------------------------------------------------

  /** `first_value(expr)` — value from the first row of the window frame. */
  def firstValue[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("first_value(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /** `last_value(expr)` — value from the last row of the window frame. */
  def lastValue[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("last_value(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /** `nth_value(expr, n)` — value from the `n`-th row of the window frame (1-based). */
  def nthValue[T](expr: TypedExpr[T], n: Int): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("nth_value(") |+| expr.render |+| TypedExpr.raw(s", $n)"), expr.codec)

}
