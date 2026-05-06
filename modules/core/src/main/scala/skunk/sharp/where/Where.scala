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
   * Normalise an `Args` type into a flat tuple shape: `Void` → `EmptyTuple`, an existing `Tuple` stays as-is,
   * and any other scalar `X` becomes `X *: EmptyTuple`. The "every Args is a tuple" intermediate form lets
   * [[Concat]] flatten via `Tuple.Concat` and produces flat user-facing tuples.
   *
   * Caveat: a leaf with tuple-typed Args (e.g. a hypothetical `Param[(Int, String)]`) is treated by `AsTuple`
   * as an already-flattened 2-slot contribution. That matches its encoder's column structure, so it composes
   * uniformly — but it means the user-facing call shape sees the tuple's elements, not the tuple itself.
   */
  type AsTuple[X] <: Tuple = X match {
    case Void  => EmptyTuple
    case Tuple => X & Tuple
    case _     => X *: EmptyTuple
  }

  /**
   * Inverse of [[AsTuple]] for the visible `Args` type: empty tuple → `Void`, single-element tuple → its
   * element, otherwise the tuple itself. Composed with `AsTuple` and `Tuple.Concat`, this gives the
   * flat-but-Void-eliding visible shape callers see at `.compile`.
   */
  type FromTuple[T <: Tuple] = T match {
    case EmptyTuple      => Void
    case h *: EmptyTuple => h
    case _               => T
  }

  /**
   * Type-level concat with `Void` elision and **flat tuple flattening**. The lhs and rhs are each normalised
   * to tuple shape via [[AsTuple]], concatenated, and unwrapped via [[FromTuple]]. Examples:
   *   - `Concat[Void, Void]              = Void`
   *   - `Concat[Void, T]                 = T`
   *   - `Concat[T, Void]                 = T`
   *   - `Concat[Int, String]             = (Int, String)`
   *   - `Concat[(Int, String), Boolean]  = (Int, String, Boolean)` ← flat (was nested before)
   *   - `Concat[Int, (String, Boolean)]  = (Int, String, Boolean)`
   *
   * Used by builders / operators to thread the combined `Args` parameter; chained `&&`/`combine` always
   * collapses to a single flat user-facing tuple.
   */
  type Concat[A, B] = FromTuple[Tuple.Concat[AsTuple[A], AsTuple[B]]]

  /**
   * Singleton-Boolean tag indicating whether `T` reduces to `Void`. Used as the scrutinee of
   * `inline scala.compiletime.constValue[IsVoidTag[T]]` to dispatch on `T`'s reduction — the upper-bound
   * `<: Boolean` and the [[constValue]] wrapper force the compiler to fully reduce nested match types
   * (`FoldConcat[(Void, Void)] = Void`, `Concat[Void, Void] = Void`, …) to a singleton `true` or `false`.
   *
   * Plain `inline erasedValue[T] match { case _: Void => … }` does NOT trigger this reduction — it
   * pattern-matches on the un-reduced match-type form and falls through to the default arm whenever `T`
   * isn't syntactically `Void`, even when it semantically reduces to `Void`.
   */
  type IsVoidTag[T] <: Boolean = T match {
    case Void => true
    case _    => false
  }

  /**
   * Companion to [[IsVoidTag]] for tuple-shape detection. `true` if `T` is a `Tuple` (including `EmptyTuple`,
   * `Tuple1[_]`, `(A, B)`, …), `false` otherwise. Used by [[projectConcat]] to choose between scalar/tuple
   * slicing of the flat result.
   */
  type IsTupleTag[T] <: Boolean = T match {
    case Tuple => true
    case _     => false
  }

  /**
   * Project a `Concat[A, B]` value (whatever shape it reduced to) back into a `(A, B)` tuple — the input shape
   * an `Encoder[A].product(Encoder[B])` actually expects at execute time. With the smart-flat `Concat`, the
   * runtime value is a flat tuple of `Tuple.Size[AsTuple[A]] + Tuple.Size[AsTuple[B]]` elements (or `Void`,
   * or one of `A` / `B` if the other side is `Void`). Splitting requires knowing `A`'s arity at compile time
   * — so this is an `inline def`, dispatched on `IsVoidTag[A]` / `IsVoidTag[B]` / `IsTupleTag[A]` /
   * `IsTupleTag[B]` via `inline constValue`. Caller must be `inline` (or have `A`/`B` concrete) so the
   * dispatch reduces.
   */
  inline def projectConcat[A, B](c: Concat[A, B]): (A, B) =
    inline scala.compiletime.constValue[IsVoidTag[A]] match
      case true =>
        inline scala.compiletime.constValue[IsVoidTag[B]] match
          case true  => (Void, Void).asInstanceOf[(A, B)]
          case false => (Void, c).asInstanceOf[(A, B)]
      case false =>
        inline scala.compiletime.constValue[IsVoidTag[B]] match
          case true  => (c, Void).asInstanceOf[(A, B)]
          case false =>
            // Both A, B non-Void. Concat reduces to a flat Tuple of size sizeA + sizeB (>= 2).
            val sizeA = scala.compiletime.constValue[Tuple.Size[AsTuple[A]]]
            val flat  = c.asInstanceOf[Tuple]
            val (aTup, bTup) = flat.splitAt(sizeA)
            val aOut = inline scala.compiletime.constValue[IsTupleTag[A]] match
              case true  => aTup
              case false => aTup.productElement(0)
            val bOut = inline scala.compiletime.constValue[IsTupleTag[B]] match
              case true  => bTup
              case false => bTup.productElement(0)
            (aOut, bOut).asInstanceOf[(A, B)]

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
