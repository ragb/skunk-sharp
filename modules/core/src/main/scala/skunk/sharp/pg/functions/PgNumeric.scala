package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor

/**
 * Math functions. Mixed into [[skunk.sharp.Pg]] alongside the other `Pg<Category>` traits — users call `Pg.abs(col)`,
 * `Pg.sqrt(col)`, etc.
 *
 * Two groups:
 *   - **Same-type functions** (`abs`, `ceil`, `floor`, `trunc`, `round`, `mod`, `greatest`, `least`) — return the
 *     input's Scala type, so tag information flows through. Postgres accepts any numeric input; a non-numeric TypedExpr
 *     raises at execution time, not at compile time.
 *   - **Fixed `Double` functions** (`sqrt`, `power`, `exp`, `ln`, `log`) — always return `double precision`. Wrapped in
 *     [[Lift]] so a nullable input stays nullable: `sqrt(TypedExpr[Option[Int]]) → TypedExpr[Option[Double]]`.
 */
trait PgNumeric {

  // -------- Same type as input ---------------------------------------------------------------------

  /** `abs(x)` — absolute value. */
  def abs[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("abs", e)

  /** `ceil(x)` — nearest integer ≥ `x`. */
  def ceil[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("ceil", e)

  /** `floor(x)` — nearest integer ≤ `x`. */
  def floor[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("floor", e)

  /** `trunc(x)` — truncate toward zero. */
  def trunc[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("trunc", e)

  /** `round(x)` — round to nearest integer. */
  def round[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("round", e)

  /** `round(x, digits)` — round to `digits` places. Postgres defines this only for `numeric`. */
  def round[T](e: TypedExpr[T], digits: Int): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("round(") |+| e.render |+| TypedExpr.raw(s", $digits)"), e.codec)

  /** `mod(a, b)` — remainder. Both arguments must share the same type. */
  def mod[T](a: TypedExpr[T], b: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("mod(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `greatest(a, b, …)` — largest of the arguments; non-null wins over null. */
  def greatest[T](args: TypedExpr[T]*): TypedExpr[T] = {
    require(args.nonEmpty, "greatest() needs at least one argument")
    TypedExpr(
      TypedExpr.raw("greatest(") |+| TypedExpr.joined(args.map(_.render).toList, ", ") |+| TypedExpr.raw(")"),
      args.head.codec
    )
  }

  /** `least(a, b, …)` — smallest of the arguments. */
  def least[T](args: TypedExpr[T]*): TypedExpr[T] = {
    require(args.nonEmpty, "least() needs at least one argument")
    TypedExpr(
      TypedExpr.raw("least(") |+| TypedExpr.joined(args.map(_.render).toList, ", ") |+| TypedExpr.raw(")"),
      args.head.codec
    )
  }

  // -------- Fixed `Double` return (NULL-propagating) ---------------------------------------------

  /** `sqrt(x)`. */
  def sqrt[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("sqrt", e)

  /** `power(a, b)` — `a` raised to `b`. */
  def power[A, B](a: TypedExpr[A], b: TypedExpr[B])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double]] =
    TypedExpr(
      TypedExpr.raw("power(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `exp(x)`. */
  def exp[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("exp", e)

  /** `ln(x)`. */
  def ln[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("ln", e)

  /** `log(x)` — base-10. */
  def log[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("log", e)

  // -------- Constants ----------------------------------------------------------------------------

  /** `pi()` — constant. */
  val pi: TypedExpr[Double] = TypedExpr(TypedExpr.raw("pi()"), skunk.codec.all.float8)

  /** `random()` — uniformly distributed in `[0, 1)`. */
  val random: TypedExpr[Double] = TypedExpr(TypedExpr.raw("random()"), skunk.codec.all.float8)

  // -------- Sign ---------------------------------------------------------------------------------

  /** `sign(x)` — `-1`, `0`, or `1`; same type as input. */
  def sign[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("sign", e)

  // -------- Degree / radian conversion ----------------------------------------------------------

  /** `degrees(x)` — convert radians to degrees. */
  def degrees[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("degrees", e)

  /** `radians(x)` — convert degrees to radians. */
  def radians[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("radians", e)

  // -------- Trigonometric (return Double, NULL-propagating) -------------------------------------

  /** `sin(x)`. */
  def sin[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("sin", e)

  /** `cos(x)`. */
  def cos[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("cos", e)

  /** `tan(x)`. */
  def tan[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("tan", e)

  /** `asin(x)`. */
  def asin[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("asin", e)

  /** `acos(x)`. */
  def acos[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("acos", e)

  /** `atan(x)`. */
  def atan[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("atan", e)

  /** `atan2(y, x)` — angle in radians between the positive x-axis and the point `(x, y)`. */
  def atan2[A, B](y: TypedExpr[A], x: TypedExpr[B])(using pf: PgTypeFor[Lift[A, Double]]): TypedExpr[Lift[A, Double]] =
    TypedExpr(
      TypedExpr.raw("atan2(") |+| y.render |+| TypedExpr.raw(", ") |+| x.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  // -------- Hyperbolic --------------------------------------------------------------------------

  /** `sinh(x)`. */
  def sinh[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("sinh", e)

  /** `cosh(x)`. */
  def cosh[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("cosh", e)

  /** `tanh(x)`. */
  def tanh[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("tanh", e)

}
