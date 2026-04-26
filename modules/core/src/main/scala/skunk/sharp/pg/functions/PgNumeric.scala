package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/**
 * Math functions. Mixed into [[skunk.sharp.Pg]] alongside the other `Pg<Category>` traits — users call `Pg.abs(col)`,
 * `Pg.sqrt(col)`, etc. Args of input expression(s) propagate to the result.
 */
trait PgNumeric {

  // -------- Same type as input ---------------------------------------------------------------------

  def abs[T, A](e: TypedExpr[T, A]): TypedExpr[T, A]   = sameTypeFn("abs", e)
  def ceil[T, A](e: TypedExpr[T, A]): TypedExpr[T, A]  = sameTypeFn("ceil", e)
  def floor[T, A](e: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("floor", e)
  def trunc[T, A](e: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("trunc", e)
  def round[T, A](e: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("round", e)

  /** `round(x, digits)` — Postgres defines this only for `numeric`. */
  def round[T, A](e: TypedExpr[T, A], digits: Int): TypedExpr[T, A] = {
    val parts = e.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s", $digits)"))
    val frag  = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("round(")) ++ parts,
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `mod(a, b)` — both arms typed; combined Args. */
  def mod[T, AA, BA](a: TypedExpr[T, AA], b: TypedExpr[T, BA]): TypedExpr[T, Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("mod(", inner, ")")
    TypedExpr[T, Where.Concat[AA, BA]](frag, a.codec)
  }

  /** `greatest(a, b, …)` — variadic; Args collapses to `?`. */
  def greatest[T](args: TypedExpr[T, ?]*): TypedExpr[T, ?] = {
    require(args.nonEmpty, "greatest() needs at least one argument")
    naryPreserve("greatest", args)
  }

  /** `least(a, b, …)` — smallest of the arguments. */
  def least[T](args: TypedExpr[T, ?]*): TypedExpr[T, ?] = {
    require(args.nonEmpty, "least() needs at least one argument")
    naryPreserve("least", args)
  }

  // -------- Fixed `Double` return (NULL-propagating) ---------------------------------------------

  def sqrt[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("sqrt", e)

  def power[A, B, AA, BA](a: TypedExpr[A, AA], b: TypedExpr[B, BA])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double], Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("power(", inner, ")")
    TypedExpr[Lift[A, Double], Where.Concat[AA, BA]](frag, pf.codec)
  }

  def exp[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("exp", e)
  def ln[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]):  TypedExpr[Lift[T, Double], A] = doubleFn("ln", e)
  def log[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("log", e)

  // -------- Constants ----------------------------------------------------------------------------

  val pi: TypedExpr[Double, Void]     = TypedExpr(TypedExpr.voidFragment("pi()"),     skunk.codec.all.float8)
  val random: TypedExpr[Double, Void] = TypedExpr(TypedExpr.voidFragment("random()"), skunk.codec.all.float8)

  // -------- Sign ---------------------------------------------------------------------------------

  def sign[T, A](e: TypedExpr[T, A]): TypedExpr[T, A] = sameTypeFn("sign", e)

  // -------- Degree / radian conversion ----------------------------------------------------------

  def degrees[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("degrees", e)
  def radians[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("radians", e)

  // -------- Trigonometric (return Double, NULL-propagating) -------------------------------------

  def sin[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]):  TypedExpr[Lift[T, Double], A] = doubleFn("sin", e)
  def cos[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]):  TypedExpr[Lift[T, Double], A] = doubleFn("cos", e)
  def tan[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]):  TypedExpr[Lift[T, Double], A] = doubleFn("tan", e)
  def asin[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("asin", e)
  def acos[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("acos", e)
  def atan[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("atan", e)

  /** `atan2(y, x)` — both arms typed; combined Args. */
  def atan2[A, B, AA, BA](y: TypedExpr[A, AA], x: TypedExpr[B, BA])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double], Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSep(y.fragment, ", ", x.fragment)
    val frag  = TypedExpr.wrap("atan2(", inner, ")")
    TypedExpr[Lift[A, Double], Where.Concat[AA, BA]](frag, pf.codec)
  }

  // -------- Hyperbolic --------------------------------------------------------------------------

  def sinh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("sinh", e)
  def cosh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("cosh", e)
  def tanh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("tanh", e)

  /** Variadic same-type fn: `name(arg, arg, ...)`. Args collapses to `?`. */
  private def naryPreserve[T](name: String, args: Seq[TypedExpr[T, ?]]): TypedExpr[T, ?] = {
    val joined = args.tail.foldLeft(args.head.fragment.asInstanceOf[Fragment[Any]]) { (acc, a) =>
      TypedExpr.combineSep(acc, ", ", a.fragment).asInstanceOf[Fragment[Any]]
    }
    val frag = TypedExpr.wrap(s"$name(", joined, ")")
    TypedExpr[T, Any](frag, args.head.codec)
  }

}
