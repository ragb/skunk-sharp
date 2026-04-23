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
        TypedExpr.parameterised(sep).render |+| TypedExpr.raw(")"),
      skunk.codec.all.text
    )

  /** `bool_and(expr)` — true if every non-null input is true. */
  def boolAnd(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_and(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

  /** `bool_or(expr)` — true if any non-null input is true. */
  def boolOr(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_or(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

  // -------- Variance / standard deviation (return type mirrors avg) ----------------------------

  /** `stddev(expr)` — sample standard deviation; return type follows Postgres's actual rules via [[AvgOf]]. */
  def stddev[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("stddev(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `stddev_pop(expr)` — population standard deviation. */
  def stddevPop[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("stddev_pop(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `stddev_samp(expr)` — sample standard deviation (synonym for [[stddev]]). */
  def stddevSamp[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("stddev_samp(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `variance(expr)` — sample variance; return type follows [[AvgOf]]. */
  def variance[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("variance(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `var_pop(expr)` — population variance. */
  def varPop[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("var_pop(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `var_samp(expr)` — sample variance (synonym for [[variance]]). */
  def varSamp[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("var_samp(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  // -------- Two-arg statistical correlations ---------------------------------------------------

  /** `corr(y, x)` — Pearson correlation coefficient. */
  def corr[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("corr", y, x)

  /** `covar_pop(y, x)` — population covariance. */
  def covarPop[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("covar_pop", y, x)

  /** `covar_samp(y, x)` — sample covariance. */
  def covarSamp[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("covar_samp", y, x)

  // -------- Regression analysis ----------------------------------------------------------------

  /** `regr_slope(y, x)` — slope of the least-squares fit line. */
  def regrSlope[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_slope", y, x)

  /** `regr_intercept(y, x)` — y-intercept of the least-squares fit line. */
  def regrIntercept[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] =
    twoArgDoubleFn("regr_intercept", y, x)

  /** `regr_count(y, x)` — number of non-null `(y, x)` pairs; never returns NULL. */
  def regrCount[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Long] =
    TypedExpr(
      TypedExpr.raw("regr_count(") |+| y.render |+| TypedExpr.raw(", ") |+| x.render |+| TypedExpr.raw(")"),
      skunk.codec.all.int8
    )

  /** `regr_r2(y, x)` — square of the correlation coefficient. */
  def regrR2[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_r2", y, x)

  /** `regr_avgx(y, x)` — average of independent variable for non-null pairs. */
  def regrAvgX[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_avgx", y, x)

  /** `regr_avgy(y, x)` — average of dependent variable for non-null pairs. */
  def regrAvgY[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_avgy", y, x)

  /** `regr_sxx(y, x)` — sum of squares of the independent variable. */
  def regrSxx[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_sxx", y, x)

  /** `regr_syy(y, x)` — sum of squares of the dependent variable. */
  def regrSyy[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_syy", y, x)

  /** `regr_sxy(y, x)` — sum of cross products. */
  def regrSxy[Y, X](y: TypedExpr[Y], x: TypedExpr[X]): TypedExpr[Double] = twoArgDoubleFn("regr_sxy", y, x)

}
