package skunk.sharp.where

import skunk.{Encoder, Fragment, Void}
import skunk.sharp.TypedExpr
import skunk.util.Origin

/**
 * `Where[A]` is a type alias for `TypedExpr[Boolean, A]` — a boolean-typed expression that contributes `A` to the
 * surrounding builder's WHERE / HAVING / ON args. Kept as a type alias for ergonomic call sites and
 * documentation; it's the same vocabulary as any other typed expression.
 */
type Where[A] = TypedExpr[Boolean, A]

/**
 * Combinator + helper namespace for `Where`. Companion-style — methods (`apply`, `Concat`, `binop`, `notOf`)
 * stay on the object even though `Where` is a type alias rather than a class.
 *
 *   - Leaves come from comparison operators: `u.age >= Param[Int]` → `Where[Int]`,
 *     `u.email === lit("x")` → `Where[Void]`.
 *   - Combinators (`&&`, `||`, `!`) compose two `Where`s by AND/OR-ing their fragments and pairing their Args via
 *     [[Where.Concat]].
 *   - Boolean-returning expressions from anywhere else (`Pg.exists(sub)`, jsonb `@>`, range `<<`, …) are also
 *     `Where`s by virtue of being `TypedExpr[Boolean, ?]`.
 */
object Where {

  private[sharp] val OR_KW:  String = " OR "
  private[sharp] val AND_KW: String = " AND "
  private[sharp] val NOT_OPEN_KW: String = "NOT ("

  /** Construct directly from a typed Fragment + codec. Codec is fixed to bool. */
  def apply[A](fragment: Fragment[A]): TypedExpr[Boolean, A] =
    TypedExpr[Boolean, A](fragment, skunk.codec.all.bool)

  /**
   * Type-level concat: drop `Void` placeholders so `(Void, A)` collapses to `A`, `(A, Void)` to `A`, and
   * `(Void, Void)` to `Void`. Used by builders / operators to keep their `Args` parameter clean when one of the
   * arms contributes no params.
   */
  type Concat[A, B] = (A, B) match {
    case (Void, Void) => Void
    case (Void, b)    => b
    case (a, Void)    => a
    case _            => (A, B)
  }

  /**
   * Pair two encoders (Void-aware). Returns `Encoder[Any]` because the result type depends on the [[Concat]]
   * reduction; callers cast at the Fragment-construction site where the typed `Args` shape is known.
   */
  private[sharp] def concatEncoders(a: Encoder[?], b: Encoder[?]): Encoder[?] =
    if (a eq Void.codec) b
    else if (b eq Void.codec) a
    else a.asInstanceOf[Encoder[Any]].product(b.asInstanceOf[Encoder[Any]])

  // ---- Combinators (binop / not) ---------------------------------------------------------------

  private[sharp] def binop[A, B](
    l: TypedExpr[Boolean, A],
    r: TypedExpr[Boolean, B],
    opSql: String
  ): TypedExpr[Boolean, Concat[A, B]] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] =
      List[Either[String, cats.data.State[Int, String]]](Left("(")) ++
        l.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(opSql)) ++
        r.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val enc                   = concatEncoders(l.fragment.encoder, r.fragment.encoder)
    val frag: Fragment[Concat[A, B]] =
      Fragment(parts, enc.asInstanceOf[Encoder[Concat[A, B]]], Origin.unknown)
    apply[Concat[A, B]](frag)
  }

  private[sharp] def notOf[A](w: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] =
      List[Either[String, cats.data.State[Int, String]]](Left(NOT_OPEN_KW)) ++
        w.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val frag: Fragment[A] = Fragment(parts, w.fragment.encoder, Origin.unknown)
    apply[A](frag)
  }

  /**
   * Adopt a `TypedExpr[Boolean, A]` as a `Where[A]` — identity now that Where is a type alias. Kept for source
   * compat with code that previously called `Where(expr)` to lift a non-Where Boolean expression.
   */
  def fromTypedExpr[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = expr

}

/** Combinator extensions on `TypedExpr[Boolean, A]`. */
extension [A](self: TypedExpr[Boolean, A]) {

  /** AND two predicates — combined `Args = Concat[A, B]`. */
  def and[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW)

  /** Infix AND. */
  def &&[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW)

  /** OR two predicates — combined `Args = Concat[A, B]`. */
  def or[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW)

  /** Infix OR. */
  def ||[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW)

  /** NOT a predicate. */
  def not: TypedExpr[Boolean, A] = Where.notOf(self)

  /** Unary NOT — same as `.not`. */
  def unary_! : TypedExpr[Boolean, A] = Where.notOf(self)

}
