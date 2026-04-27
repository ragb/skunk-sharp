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
 * Combinator + helper namespace for `Where`.
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
   * Project a `Concat[A, B]` value (whatever shape that reduced to) back into a `(A, B)` tuple — the input
   * shape an `Encoder[A].product(Encoder[B])` actually expects at execute time. Without this, an
   * `Encoder[(A, B)]` cast as `Encoder[Concat[A, B]]` would receive the wrong shape (e.g. raw `Void` when
   * `Concat[Void, Void] = Void` reduced) and ClassCastException deep in Skunk's encoder chain.
   *
   * The four arms mirror the [[Concat]] match type. Resolved at the call site where `A` and `B` are concrete,
   * via the priority chain below.
   */
  trait Concat2[A, B] {
    def project(c: Concat[A, B]): (A, B)
  }

  object Concat2 extends Concat2HighPrio {
    /** Both arms contribute `Void`: `Concat[Void, Void] = Void`, supply `(Void, Void)`. */
    given bothVoid: Concat2[Void, Void] = new Concat2[Void, Void] {
      def project(c: Concat[Void, Void]): (Void, Void) = (Void, Void)
    }
  }

  trait Concat2HighPrio extends Concat2MedPrio {
    /** LHS is Void: `Concat[Void, B] = B`. Supply `(Void, b)`. */
    given leftVoid[B]: Concat2[Void, B] = new Concat2[Void, B] {
      def project(c: Concat[Void, B]): (Void, B) = (Void, c.asInstanceOf[B])
    }
    /** RHS is Void: `Concat[A, Void] = A`. Supply `(a, Void)`. */
    given rightVoid[A]: Concat2[A, Void] = new Concat2[A, Void] {
      def project(c: Concat[A, Void]): (A, Void) = (c.asInstanceOf[A], Void)
    }
  }

  trait Concat2MedPrio {
    /** Default: neither side is Void. `Concat[A, B] = (A, B)` — identity projection. */
    given default[A, B]: Concat2[A, B] = new Concat2[A, B] {
      def project(c: Concat[A, B]): (A, B) = c.asInstanceOf[(A, B)]
    }
  }

  /**
   * Pair two encoders into one whose input shape matches `Concat[A, B]`. Always products both sub-encoders
   * (so any baked values riding on either side flow through), then contramaps the user's `Concat[A, B]` input
   * back into the `(A, B)` tuple the product actually consumes.
   */
  private[sharp] def concatEncoders[A, B](
    a: Encoder[?], b: Encoder[?]
  )(using c2: Concat2[A, B]): Encoder[Concat[A, B]] = {
    val productEnc: Encoder[(Any, Any)] =
      a.asInstanceOf[Encoder[Any]].product(b.asInstanceOf[Encoder[Any]])
    productEnc.contramap[Concat[A, B]](in => c2.project(in).asInstanceOf[(Any, Any)])
  }

  // ---- Combinators (binop / not) ---------------------------------------------------------------

  private[sharp] def binop[A, B](
    l: TypedExpr[Boolean, A],
    r: TypedExpr[Boolean, B],
    opSql: String
  )(using c2: Concat2[A, B]): TypedExpr[Boolean, Concat[A, B]] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] =
      List[Either[String, cats.data.State[Int, String]]](Left("(")) ++
        l.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(opSql)) ++
        r.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val enc                           = concatEncoders[A, B](l.fragment.encoder, r.fragment.encoder)
    val frag: Fragment[Concat[A, B]]  = Fragment(parts, enc, Origin.unknown)
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
  def and[B](that: TypedExpr[Boolean, B])(using Where.Concat2[A, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW)

  /** Infix AND. */
  def &&[B](that: TypedExpr[Boolean, B])(using Where.Concat2[A, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW)

  /** OR two predicates — combined `Args = Concat[A, B]`. */
  def or[B](that: TypedExpr[Boolean, B])(using Where.Concat2[A, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW)

  /** Infix OR. */
  def ||[B](that: TypedExpr[Boolean, B])(using Where.Concat2[A, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW)

  /** NOT a predicate. */
  def not: TypedExpr[Boolean, A] = Where.notOf(self)

  /** Unary NOT — same as `.not`. */
  def unary_! : TypedExpr[Boolean, A] = Where.notOf(self)

}
