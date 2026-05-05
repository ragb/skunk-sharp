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
   * Project a `Concat[A, B]` value (whatever shape it reduced to) back into a `(A, B)` tuple — the input shape
   * an `Encoder[A].product(Encoder[B])` actually expects at execute time. Without this, an `Encoder[(A, B)]`
   * cast as `Encoder[Concat[A, B]]` would receive the wrong shape (e.g. raw `Void` when `Concat[Void, Void] =
   * Void` reduced) and ClassCastException deep in Skunk's encoder chain.
   *
   * The four arms mirror the [[Concat]] match type. Inline-dispatched on `A` / `B` shape via
   * `compiletime.erasedValue` — caller must therefore be `inline` (or have `A`/`B` concrete in its scope) so
   * the dispatch reduces.
   */
  inline def projectConcat[A, B](c: Concat[A, B]): (A, B) =
    inline scala.compiletime.erasedValue[A] match
      case _: Void =>
        inline scala.compiletime.erasedValue[B] match
          case _: Void => (Void, Void).asInstanceOf[(A, B)]
          case _       => (Void, c).asInstanceOf[(A, B)]
      case _ =>
        inline scala.compiletime.erasedValue[B] match
          case _: Void => (c, Void).asInstanceOf[(A, B)]
          case _       => c.asInstanceOf[(A, B)]

  /**
   * Right-fold of [[Concat]] over a tuple of Args types. Drops `Void` slots cleanly so
   * `FoldConcat[(Void, Int, Void, String)] = (Int, String)`. Used by variadic builders / projection lists /
   * RETURNING tuples to combine N typed-Args slots into one.
   */
  type FoldConcat[T <: Tuple] = T match {
    case EmptyTuple        => Void
    case h *: EmptyTuple   => h
    case h *: t            => Concat[h, FoldConcat[t]]
  }

  /**
   * Project a `FoldConcat[T]` value back into a heterogeneous list of per-slot values, in tuple order — one
   * entry per slot of `T`, including `Void` placeholders for slots whose Args is `Void`. Inline-dispatched on
   * the tuple shape — caller must therefore be `inline` (or have `T` concrete) so the recursion reduces.
   */
  inline def projectFoldConcat[T <: Tuple](c: FoldConcat[T]): List[Any] =
    inline scala.compiletime.erasedValue[T] match
      case _: EmptyTuple        => Nil
      case _: (h *: EmptyTuple) => List(c)
      case _: (h *: t)          =>
        val pair = projectConcat[h, FoldConcat[t & Tuple]](c.asInstanceOf[Concat[h, FoldConcat[t & Tuple]]])
        pair._1 :: projectFoldConcat[t & Tuple](pair._2)

  /**
   * Pair two encoders into one whose input shape matches `Concat[A, B]`. The caller-supplied `proj` re-pairs
   * the `Concat[A, B]` value back into `(A, B)` — typically `c => projectConcat[A, B](c)` materialised at the
   * caller's inline expansion site so the dispatch reduces with concrete `A` / `B`.
   */
  private[sharp] def concatEncoders[A, B](
    a: Encoder[?], b: Encoder[?], proj: Concat[A, B] => (A, B)
  ): Encoder[Concat[A, B]] = {
    val productEnc: Encoder[(Any, Any)] =
      a.asInstanceOf[Encoder[Any]].product(b.asInstanceOf[Encoder[Any]])
    productEnc.contramap[Concat[A, B]](in => proj(in).asInstanceOf[(Any, Any)])
  }

  // ---- Combinators (binop / not) ---------------------------------------------------------------

  private[sharp] def binop[A, B](
    l: TypedExpr[Boolean, A],
    r: TypedExpr[Boolean, B],
    opSql: String,
    proj: Concat[A, B] => (A, B)
  ): TypedExpr[Boolean, Concat[A, B]] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] =
      List[Either[String, cats.data.State[Int, String]]](Left("(")) ++
        l.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(opSql)) ++
        r.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val enc                           = concatEncoders[A, B](l.fragment.encoder, r.fragment.encoder, proj)
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
  inline def and[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW, c => Where.projectConcat[A, B](c))

  /** Infix AND. */
  inline def &&[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW, c => Where.projectConcat[A, B](c))

  /** OR two predicates — combined `Args = Concat[A, B]`. */
  inline def or[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW, c => Where.projectConcat[A, B](c))

  /** Infix OR. */
  inline def ||[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW, c => Where.projectConcat[A, B](c))

  /** NOT a predicate. */
  def not: TypedExpr[Boolean, A] = Where.notOf(self)

  /** Unary NOT — same as `.not`. */
  def unary_! : TypedExpr[Boolean, A] = Where.notOf(self)

}
