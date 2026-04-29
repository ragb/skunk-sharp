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

  /** `greatest(a)` — single arg; Args propagates from `a`. */
  def greatest[T, A1](a: TypedExpr[T, A1]): TypedExpr[T, A1] = {
    val frag = TypedExpr.wrap("greatest(", a.fragment, ")")
    TypedExpr[T, A1](frag, a.codec)
  }

  /** `greatest(a, b)` — Args = `Concat[A1, A2]`. */
  def greatest[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2])(using
    c2: Where.Concat2[A1, A2]
  ): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("greatest(", inner, ")")
    TypedExpr[T, Where.Concat[A1, A2]](frag, a.codec)
  }

  /** `greatest(a, b, c)` — Args = `Concat[Concat[A1, A2], A3]` (left-fold). */
  def greatest[T, A1, A2, A3](a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3])(using
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], A3]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = c123.project(combined)
      val (a1v, a2v) = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("greatest(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, a.codec)
  }

  /** `greatest(a, b, c, d, …)` — variadic fallback for arity > 3; Args = `Void`. */
  def greatest[T](args: TypedExpr[T, ?]*): TypedExpr[T, Void] = {
    require(args.nonEmpty, "greatest() needs at least one argument")
    naryPreserve("greatest", args)
  }

  /** `least(a)` — single arg; Args propagates from `a`. */
  def least[T, A1](a: TypedExpr[T, A1]): TypedExpr[T, A1] = {
    val frag = TypedExpr.wrap("least(", a.fragment, ")")
    TypedExpr[T, A1](frag, a.codec)
  }

  /** `least(a, b)` — Args = `Concat[A1, A2]`. */
  def least[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2])(using
    c2: Where.Concat2[A1, A2]
  ): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("least(", inner, ")")
    TypedExpr[T, Where.Concat[A1, A2]](frag, a.codec)
  }

  /** `least(a, b, c)` — Args = `Concat[Concat[A1, A2], A3]` (left-fold). */
  def least[T, A1, A2, A3](a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3])(using
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], A3]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = c123.project(combined)
      val (a1v, a2v) = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("least(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, a.codec)
  }

  /** `least(a, b, c, d, …)` — variadic fallback for arity > 3; Args = `Void`. */
  def least[T](args: TypedExpr[T, ?]*): TypedExpr[T, Void] = {
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

  /** Variadic same-type fn: `name(arg, arg, ...)`. Args = Void (variadic — see [[TypedExpr.joinedVoid]]). */
  private def naryPreserve[T](name: String, args: Seq[TypedExpr[T, ?]]): TypedExpr[T, Void] = {
    val joined = TypedExpr.joinedVoid(", ", args.toList.map(_.fragment))
    val frag   = TypedExpr.wrap(s"$name(", joined, ")")
    TypedExpr[T, Void](frag, args.head.codec)
  }

}
