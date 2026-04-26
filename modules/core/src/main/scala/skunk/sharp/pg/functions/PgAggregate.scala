package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.util.Origin

/** Aggregate functions. Mixed into [[skunk.sharp.Pg]]. Args of input expression(s) propagate to the result. */
trait PgAggregate {

  /** `count(*)` — row count including NULLs. Args = Void. */
  val countAll: TypedExpr[Long, Void] = {
    val frag: Fragment[Void] = TypedExpr.voidFragment("count(*)")
    TypedExpr[Long, Void](frag, skunk.codec.all.int8)
  }

  /** `count(expr)`. Args propagates from input. */
  def count[T, A](expr: TypedExpr[T, A]): TypedExpr[Long, A] = {
    val frag = TypedExpr.wrap("count(", expr.fragment, ")")
    TypedExpr[Long, A](frag, skunk.codec.all.int8)
  }

  /** `count(DISTINCT expr)`. */
  def countDistinct[T, A](expr: TypedExpr[T, A]): TypedExpr[Long, A] = {
    val frag = TypedExpr.wrap("count(DISTINCT ", expr.fragment, ")")
    TypedExpr[Long, A](frag, skunk.codec.all.int8)
  }

  def sum[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[SumOf[I]]): TypedExpr[SumOf[I], A] = {
    val frag = TypedExpr.wrap("sum(", expr.fragment, ")")
    TypedExpr[SumOf[I], A](frag, pf.codec)
  }

  def avg[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("avg(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def min[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("min", expr)
  def max[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("max", expr)

  /** `string_agg(expr, sep)` — sep is a runtime value baked via Param.bind. */
  def stringAgg[T, A](expr: TypedExpr[T, A], sep: String)(using StrLike[T]): TypedExpr[String, A] = {
    val sepFrag = Param.bind[String](sep)(using PgTypeFor.stringPgTypeFor).fragment
    val inner   = TypedExpr.combineSep(expr.fragment, ", ", sepFrag)
    val frag    = TypedExpr.wrap("string_agg(", inner.asInstanceOf[Fragment[A]], ")")
    TypedExpr[String, A](frag, skunk.codec.all.text)
  }

  /** `string_agg` taking a TypedExpr separator (Param[String], lit, etc.) — Args propagate from both. */
  def stringAgg[T, A, B](expr: TypedExpr[T, A], sep: TypedExpr[String, B])(using
    StrLike[T]
  ): TypedExpr[String, Where.Concat[A, B]] = {
    val inner = TypedExpr.combineSep(expr.fragment, ", ", sep.fragment)
    val frag  = TypedExpr.wrap("string_agg(", inner, ")")
    TypedExpr[String, Where.Concat[A, B]](frag, skunk.codec.all.text)
  }

  def boolAnd[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = sameTypeFn("bool_and", expr)
  def boolOr[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A]  = sameTypeFn("bool_or", expr)

  // -------- Variance / standard deviation -----------------------------------------------------

  def stddev[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("stddev(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def stddevPop[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("stddev_pop(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def stddevSamp[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("stddev_samp(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def variance[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("variance(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def varPop[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("var_pop(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  def varSamp[I, A](expr: TypedExpr[I, A])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I], A] = {
    val frag = TypedExpr.wrap("var_samp(", expr.fragment, ")")
    TypedExpr[AvgOf[I], A](frag, pf.codec)
  }

  // -------- Two-arg statistical correlations -------------------------------------------------

  def corr[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("corr", y, x)

  def covarPop[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("covar_pop", y, x)

  def covarSamp[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("covar_samp", y, x)

  // -------- Regression analysis --------------------------------------------------------------

  def regrSlope[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_slope", y, x)

  def regrIntercept[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_intercept", y, x)

  def regrCount[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Long, Where.Concat[AY, AX]] = {
    val inner = TypedExpr.combineSep(y.fragment, ", ", x.fragment)
    val frag  = TypedExpr.wrap("regr_count(", inner, ")")
    TypedExpr[Long, Where.Concat[AY, AX]](frag, skunk.codec.all.int8)
  }

  def regrR2[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_r2", y, x)

  def regrAvgX[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_avgx", y, x)

  def regrAvgY[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_avgy", y, x)

  def regrSxx[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_sxx", y, x)

  def regrSyy[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_syy", y, x)

  def regrSxy[Y, X, AY, AX](y: TypedExpr[Y, AY], x: TypedExpr[X, AX]): TypedExpr[Double, Where.Concat[AY, AX]] =
    twoArgDoubleFn("regr_sxy", y, x)

}
