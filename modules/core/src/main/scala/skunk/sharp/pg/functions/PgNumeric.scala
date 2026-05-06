package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.{PgFunction, TypedExpr}
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
  inline def round[T, A1, A2](
    e:      TypedExpr[T, A1],
    digits: TypedExpr[Int, A2]
  ): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", digits.fragment)
    val frag  = TypedExpr.wrap("round(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `mod(a, b)` — both arms typed; combined Args. */
  inline def mod[T, AA, BA](a: TypedExpr[T, AA], b: TypedExpr[T, BA]): TypedExpr[T, Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSepInl[AA, BA](a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("mod(", inner, ")")
    TypedExpr[T, Where.Concat[AA, BA]](frag, a.codec)
  }

  /** `greatest(a)` — single arg; Args propagates from `a`. */
  def greatest[T, A1](a: TypedExpr[T, A1]): TypedExpr[T, A1] = {
    val frag = TypedExpr.wrap("greatest(", a.fragment, ")")
    TypedExpr[T, A1](frag, a.codec)
  }

  /** `greatest(a, b)` — Args = `Concat[A1, A2]`. */
  inline def greatest[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("greatest(", inner, ")")
    TypedExpr[T, Where.Concat[A1, A2]](frag, a.codec)
  }

  /** `greatest(a, b, c)` — `Args` flattens to `(A1, A2, A3)` via `Where.Concat` (Void slots dropped). */
  inline def greatest[T, A1, A2, A3](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = Where.projectConcat[Where.Concat[A1, A2], A3](combined)
      val (a1v, a2v) = Where.projectConcat[A1, A2](a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("greatest(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, a.codec)
  }

  /** `greatest(a, b, c, d)` — `Args` flattens to the non-Void slots of `(A1, A2, A3, A4)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: EmptyTuple](
      "greatest", List(a.fragment, b.fragment, c.fragment, d.fragment), a.codec
    )

  /** `greatest(a, b, c, d, e)` — `Args` flattens to the non-Void slots of `(A1…A5)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4, A5](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4], e: TypedExpr[T, A5]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple](
      "greatest", List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment), a.codec
    )

  /** `greatest(a, b, c, d, e, f)` — `Args` flattens to the non-Void slots of `(A1…A6)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4, A5, A6](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple](
      "greatest", List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment), a.codec
    )

  /** `greatest(a, b, c, d, e, f, g)` — `Args` flattens to the non-Void slots of `(A1…A7)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4, A5, A6, A7](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple](
      "greatest",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment),
      a.codec
    )

  /** `greatest(a, b, c, d, e, f, g, h)` — `Args` flattens to the non-Void slots of `(A1…A8)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4, A5, A6, A7, A8](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7], h: TypedExpr[T, A8]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple](
      "greatest",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment),
      a.codec
    )

  /** `greatest(a, b, c, d, e, f, g, h, i)` — `Args` flattens to the non-Void slots of `(A1…A9)` via `Where.Concat`.  */
  inline def greatest[T, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7], h: TypedExpr[T, A8], i: TypedExpr[T, A9]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple](
      "greatest",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment, i.fragment),
      a.codec
    )

  /** `least(a)` — single arg; Args propagates from `a`. */
  def least[T, A1](a: TypedExpr[T, A1]): TypedExpr[T, A1] = {
    val frag = TypedExpr.wrap("least(", a.fragment, ")")
    TypedExpr[T, A1](frag, a.codec)
  }

  /** `least(a, b)` — Args = `Concat[A1, A2]`. */
  inline def least[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("least(", inner, ")")
    TypedExpr[T, Where.Concat[A1, A2]](frag, a.codec)
  }

  /** `least(a, b, c)` — `Args` flattens to `(A1, A2, A3)` via `Where.Concat` (Void slots dropped). */
  inline def least[T, A1, A2, A3](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = Where.projectConcat[Where.Concat[A1, A2], A3](combined)
      val (a1v, a2v) = Where.projectConcat[A1, A2](a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("least(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, a.codec)
  }

  /** `least(a, b, c, d)` — `Args` flattens to the non-Void slots of `(A1, A2, A3, A4)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: EmptyTuple](
      "least", List(a.fragment, b.fragment, c.fragment, d.fragment), a.codec
    )

  /** `least(a, b, c, d, e)` — `Args` flattens to the non-Void slots of `(A1…A5)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4, A5](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4], e: TypedExpr[T, A5]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple](
      "least", List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment), a.codec
    )

  /** `least(a, b, c, d, e, f)` — `Args` flattens to the non-Void slots of `(A1…A6)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4, A5, A6](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple](
      "least", List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment), a.codec
    )

  /** `least(a, b, c, d, e, f, g)` — `Args` flattens to the non-Void slots of `(A1…A7)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4, A5, A6, A7](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple](
      "least",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment),
      a.codec
    )

  /** `least(a, b, c, d, e, f, g, h)` — `Args` flattens to the non-Void slots of `(A1…A8)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4, A5, A6, A7, A8](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7], h: TypedExpr[T, A8]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple](
      "least",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment),
      a.codec
    )

  /** `least(a, b, c, d, e, f, g, h, i)` — `Args` flattens to the non-Void slots of `(A1…A9)` via `Where.Concat`.  */
  inline def least[T, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3], d: TypedExpr[T, A4],
    e: TypedExpr[T, A5], f: TypedExpr[T, A6], g: TypedExpr[T, A7], h: TypedExpr[T, A8], i: TypedExpr[T, A9]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple](
      "least",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment, i.fragment),
      a.codec
    )

  // -------- Fixed `Double` return (NULL-propagating) ---------------------------------------------

  def sqrt[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("sqrt", e)

  inline def power[A, B, AA, BA](a: TypedExpr[A, AA], b: TypedExpr[B, BA])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double], Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSepInl[AA, BA](a.fragment, ", ", b.fragment)
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
  inline def atan2[A, B, AA, BA](y: TypedExpr[A, AA], x: TypedExpr[B, BA])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double], Where.Concat[AA, BA]] = {
    val inner = TypedExpr.combineSepInl[AA, BA](y.fragment, ", ", x.fragment)
    val frag  = TypedExpr.wrap("atan2(", inner, ")")
    TypedExpr[Lift[A, Double], Where.Concat[AA, BA]](frag, pf.codec)
  }

  // -------- Hyperbolic --------------------------------------------------------------------------

  def sinh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("sinh", e)
  def cosh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("cosh", e)
  def tanh[T, A](e: TypedExpr[T, A])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double], A] = doubleFn("tanh", e)

}
