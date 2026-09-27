package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/** Aggregate functions. Mixed into [[skunk.sharp.Pg]]. Args of input expression(s) propagate to the result. */
trait PgAggregate {

  /** `count(*)` — row count including NULLs. Args = Void. */
  val countAll: TypedExpr[Long, Void] = {
    val frag: Fragment[Void] = TypedExpr.voidFragment("count(*)")
    TypedExpr[Long, Void](frag, skunk.codec.all.int8)
  }

  /** `count(expr)`. Args propagates from input. */
  def count[T, A](expr: TypedExpr[T, A]): TypedExpr[Long, A] = {
    PgFunction.call1("count", expr, skunk.codec.all.int8)
  }

  /** `count(DISTINCT expr)`. */
  def countDistinct[T, A](expr: TypedExpr[T, A]): TypedExpr[Long, A] = {
    val frag = TypedExpr.wrap("count(DISTINCT ", expr.fragment, ")")
    TypedExpr[Long, A](frag, skunk.codec.all.int8)
  }

  def sum[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[SumOf[I]]): TypedExpr[SumOf[I], A] = {
    PgFunction.call1("sum", expr, pf.codec)
  }

  def avg[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("avg", expr, pf.codec)
  }

  def min[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("min", expr)
  def max[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("max", expr)

  /** `string_agg(expr, sep)` — Args propagate from both. */
  inline def stringAgg[T, A, B](expr: TypedExpr[T, A], sep: TypedExpr[String, B])(using
    StrLike[T]
  ): TypedExpr[String, Where.Concat[A, B]] =
    PgFunction.call2("string_agg", expr, sep, skunk.codec.all.text)

  def boolAnd[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = sameTypeFn("bool_and", expr)
  def boolOr[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A]  = sameTypeFn("bool_or", expr)

  // -------- Variance / standard deviation -----------------------------------------------------

  def stddev[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("stddev", expr, pf.codec)
  }

  def stddevPop[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("stddev_pop", expr, pf.codec)
  }

  def stddevSamp[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("stddev_samp", expr, pf.codec)
  }

  def variance[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("variance", expr, pf.codec)
  }

  def varPop[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("var_pop", expr, pf.codec)
  }

  def varSamp[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    PgFunction.call1("var_samp", expr, pf.codec)
  }

  // -------- Two-arg statistical correlations -------------------------------------------------

  inline def corr[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("corr", y, x, skunk.codec.all.float8)

  inline def covarPop[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("covar_pop", y, x, skunk.codec.all.float8)

  inline def covarSamp[Y, X, AY, AX](
    y: TypedExpr[Y, AY],
    x: TypedExpr[X, AX]
  ): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("covar_samp", y, x, skunk.codec.all.float8)

  // -------- Regression analysis --------------------------------------------------------------

  inline def regrSlope[Y, X, AY, AX](
    y: TypedExpr[Y, AY],
    x: TypedExpr[X, AX]
  ): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_slope", y, x, skunk.codec.all.float8)

  inline def regrIntercept[Y, X, AY, AX](
    y: TypedExpr[Y, AY],
    x: TypedExpr[X, AX]
  ): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_intercept", y, x, skunk.codec.all.float8)

  inline def regrCount[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Long, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_count", y, x, skunk.codec.all.int8)

  inline def regrR2[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_r2", y, x, skunk.codec.all.float8)

  inline def regrAvgX[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_avgx", y, x, skunk.codec.all.float8)

  inline def regrAvgY[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_avgy", y, x, skunk.codec.all.float8)

  inline def regrSxx[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_sxx", y, x, skunk.codec.all.float8)

  inline def regrSyy[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_syy", y, x, skunk.codec.all.float8)

  inline def regrSxy[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    PgFunction.call2("regr_sxy", y, x, skunk.codec.all.float8)

}
