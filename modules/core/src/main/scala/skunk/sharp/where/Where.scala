package skunk.sharp.where

import skunk.{Fragment, Void}
import skunk.sharp.TypedExpr

/**
 * `Where[A]` is a type alias for `TypedExpr[Boolean, A]` — a boolean-typed expression that contributes `A` to the
 * surrounding builder's WHERE / HAVING / ON args. Kept as a type alias for ergonomic call sites and documentation; it's
 * the same vocabulary as any other typed expression.
 */
type Where[A] = TypedExpr[Boolean, A]

/**
 * Combinator + helper namespace for `Where`.
 */
/**
 * Precomputed splitter for a flat Args value over slots with the given [[Where.SlotCode]]s (see [[Where.splitFlat]]).
 * Built once per `.compile` / combinator; `apply` runs at encode time.
 */
final class SlotSplit(codes: Tuple) extends (Any => IArray[Any]) {
  private val cs: Array[Int]       = codes.productIterator.map(_.asInstanceOf[Int]).toArray
  private val total: Int           = cs.foldLeft(0)((n, c) => n + width(c))
  private val allVoid: IArray[Any] = IArray.unsafeFromArray(Array.fill[Any](cs.length)(Void))

  private def width(c: Int): Int = if (c == -1) 0 else if (c == -2) 1 else c

  /** Every slot's value. */
  def apply(args: Any): IArray[Any] = from(args, 0)

  /** The values of slots `start ..` (earlier slots are skipped, not materialised). */
  def from(args: Any, start: Int): IArray[Any] =
    if (total == 0 && start == 0) allVoid
    else {
      val out = new Array[Any](cs.length - start)
      var pos = 0
      var i   = 0
      while (i < cs.length) {
        val c = cs(i)
        if (i >= start) out(i - start) = value(args, c, pos)
        pos += width(c)
        i += 1
      }
      IArray.unsafeFromArray(out)
    }

  /** Slot `i`'s value alone. */
  def at(args: Any, i: Int): Any = {
    var pos = 0
    var k   = 0
    while (k < i) { pos += width(cs(k)); k += 1 }
    value(args, cs(i), pos)
  }

  /** Every slot's value, as a List (for `combineList` projectors). */
  def toList(args: Any): List[Any] = {
    var acc = List.empty[Any]
    var pos = total
    var i   = cs.length - 1
    while (i >= 0) {
      val c = cs(i)
      pos -= width(c)
      acc = value(args, c, pos) :: acc
      i -= 1
    }
    acc
  }

  private def value(args: Any, c: Int, pos: Int): Any =
    if (c == -1) Void
    else if (c == -2) elem(args, pos)
    else if (c == total && c >= 2) args // the slot is the whole flat tuple: pass it through, no copy
    else {
      val arr = new Array[Object](c)
      var k   = 0
      while (k < c) { arr(k) = elem(args, pos + k).asInstanceOf[Object]; k += 1 }
      Tuple.fromArray(arr)
    }

  // Concat unwraps a single-element result, so the flat value is the element itself when `total == 1`.
  private def elem(args: Any, i: Int): Any =
    if (total == 1) args else args.asInstanceOf[Product].productElement(i)

}

object Where {

  private[sharp] val OR_KW: String       = " OR "
  private[sharp] val AND_KW: String      = " AND "
  private[sharp] val NOT_OPEN_KW: String = "NOT ("

  /** Construct directly from a typed Fragment + codec. Codec is fixed to bool. */
  def apply[A](fragment: Fragment[A]): TypedExpr[Boolean, A] =
    TypedExpr[Boolean, A](fragment, skunk.codec.all.bool)

  /**
   * Normalise an `Args` type into a flat tuple shape: `Void` → `EmptyTuple`, an existing `Tuple` stays as-is, and any
   * other scalar `X` becomes `X *: EmptyTuple`. The "every Args is a tuple" intermediate form lets [[Concat]] flatten
   * via `Tuple.Concat` and produces flat user-facing tuples.
   *
   * Caveat: a leaf with tuple-typed Args (e.g. a hypothetical `Param[(Int, String)]`) is treated by `AsTuple` as an
   * already-flattened 2-slot contribution. That matches its encoder's column structure, so it composes uniformly — but
   * it means the user-facing call shape sees the tuple's elements, not the tuple itself.
   */
  type AsTuple[X] <: Tuple = X match {
    case Void  => EmptyTuple
    case Tuple => X & Tuple
    case _     => X *: EmptyTuple
  }

  /**
   * Inverse of [[AsTuple]] for the visible `Args` type: empty tuple → `Void`, single-element tuple → its element,
   * otherwise the tuple itself. Composed with `AsTuple` and `Tuple.Concat`, this gives the flat-but-Void-eliding
   * visible shape callers see at `.compile`.
   */
  type FromTuple[T <: Tuple] = T match {
    case EmptyTuple      => Void
    case h *: EmptyTuple => h
    case _               => T
  }

  /**
   * Type-level concat with `Void` elision and **flat tuple flattening**. The lhs and rhs are each normalised to tuple
   * shape via [[AsTuple]], concatenated, and unwrapped via [[FromTuple]]. Examples:
   *   - `Concat[Void, Void]              = Void`
   *   - `Concat[Void, T]                 = T`
   *   - `Concat[T, Void]                 = T`
   *   - `Concat[Int, String]             = (Int, String)`
   *   - `Concat[(Int, String), Boolean]  = (Int, String, Boolean)` ← flat (was nested before)
   *   - `Concat[Int, (String, Boolean)]  = (Int, String, Boolean)`
   *
   * Used by builders / operators to thread the combined `Args` parameter; chained `&&`/`combine` always collapses to a
   * single flat user-facing tuple.
   */
  type Concat[A, B] = FromTuple[Tuple.Concat[AsTuple[A], AsTuple[B]]]

  /**
   * Singleton-Boolean tag indicating whether `T` reduces to `Void`. Used as the scrutinee of
   * `inline scala.compiletime.constValue[IsVoidTag[T]]` to dispatch on `T`'s reduction — the upper-bound `<: Boolean`
   * and the [[constValue]] wrapper force the compiler to fully reduce nested match types (`FoldConcat[(Void, Void)] =
   * Void`, `Concat[Void, Void] = Void`, …) to a singleton `true` or `false`.
   *
   * Plain `inline erasedValue[T] match { case _: Void => … }` does NOT trigger this reduction — it pattern-matches on
   * the un-reduced match-type form and falls through to the default arm whenever `T` isn't syntactically `Void`, even
   * when it semantically reduces to `Void`.
   */
  type IsVoidTag[T] <: Boolean = T match {
    case Void => true
    case _    => false
  }

  /**
   * Companion to [[IsVoidTag]] for tuple-shape detection. `true` if `T` is a `Tuple` (including `EmptyTuple`,
   * `Tuple1[_]`, `(A, B)`, …), `false` otherwise. Used by [[projectConcat]] to choose between scalar/tuple slicing of
   * the flat result.
   */
  type IsTupleTag[T] <: Boolean = T match {
    case Tuple => true
    case _     => false
  }

  /**
   * Project a `Concat[A, B]` value (whatever shape it reduced to) back into a `(A, B)` tuple — the input shape an
   * `Encoder[A].product(Encoder[B])` actually expects at execute time. With the smart-flat `Concat`, the runtime value
   * is a flat tuple of `Tuple.Size[AsTuple[A]] + Tuple.Size[AsTuple[B]]` elements (or `Void`, or one of `A` / `B` if
   * the other side is `Void`). Splitting requires knowing `A`'s arity at compile time — so this is an `inline def`,
   * dispatched on `IsVoidTag[A]` / `IsVoidTag[B]` / `IsTupleTag[A]` / `IsTupleTag[B]` via `inline constValue`. Caller
   * must be `inline` (or have `A`/`B` concrete) so the dispatch reduces.
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
            val sizeA        = scala.compiletime.constValue[Tuple.Size[AsTuple[A]]]
            val flat         = c.asInstanceOf[Tuple]
            val (aTup, bTup) = flat.splitAt(sizeA)
            val aOut         = inline scala.compiletime.constValue[IsTupleTag[A]] match
              case true  => aTup
              case false => aTup.productElement(0)
            val bOut = inline scala.compiletime.constValue[IsTupleTag[B]] match
              case true  => bTup
              case false => bTup.productElement(0)
            (aOut, bOut).asInstanceOf[(A, B)]

  /**
   * Shape code of one Args slot, for [[splitFlat]]: `-1` = `Void` (contributes nothing), `-2` = a scalar (one element,
   * unwrapped), `n >= 0` = an `n`-tuple (n elements, kept as a tuple).
   */
  type SlotCode[X] <: Int = X match {
    case Void  => -1
    case Tuple => Tuple.Size[X & Tuple]
    case _     => -2
  }

  type SlotCodes[T <: Tuple] <: Tuple = T match {
    case EmptyTuple => EmptyTuple
    case h *: t     => SlotCode[h] *: SlotCodes[t]
  }

  /** The [[SlotCode]]s of a tuple of slot Args types, as a compile-time constant. */
  inline def slotCodes[T <: Tuple]: Tuple = scala.compiletime.constValueTuple[SlotCodes[T]]

  /**
   * Split a flat `FoldConcat`-shaped Args value back into one value per slot, given the slots' [[SlotCode]]s. The
   * runtime equivalent of a chain of [[projectConcat]]s — `Concat` is associative over the slots' flattened shapes —
   * but one shared non-inline body instead of a nested inline expansion at every call site.
   */
  def splitFlat(args: Any, codes: Tuple): IArray[Any] =
    new SlotSplit(codes)(args) // one-off use; hot paths hold a SlotSplit

  /** A splitter for Concat[A, B] values — the thin, shared counterpart of `c => projectConcat[A, B](c)`. */
  inline def projPair[A, B]: Concat[A, B] => (A, B) = pairOf[A, B](slotCodes[(A, B)])

  /** A splitter for FoldConcat[T] values — the thin, shared counterpart of `c => projectFoldConcat[T](c)`. */
  inline def projFold[T <: Tuple]: FoldConcat[T] => List[Any] = foldOf[FoldConcat[T]](slotCodes[T])

  def foldOf[C](codes: Tuple): C => List[Any] = {
    val sp = new SlotSplit(codes)
    c => sp.toList(c)
  }

  /** Pair splitter, specialised by shape at construction so the common cases allocate only the result pair. */
  def pairOf[A, B](codes: Tuple): Concat[A, B] => (A, B) = {
    val a                    = codes.productElement(0).asInstanceOf[Int]
    val b                    = codes.productElement(1).asInstanceOf[Int]
    val f: Any => (Any, Any) =
      if (a == -1 && b == -1) { val vv = (Void, Void); _ => vv }
      else if (a == -1 && b == -2) c => (Void, c)
      else if (a == -2 && b == -1) c => (c, Void)
      else if (a == -2 && b == -2) c => { val t = c.asInstanceOf[Product]; (t.productElement(0), t.productElement(1)) }
      else { val sp = new SlotSplit(codes); c => { val v = sp(c); (v(0), v(1)) } }
    f.asInstanceOf[Concat[A, B] => (A, B)]
  }

  /**
   * Right-fold of [[Concat]] over a tuple of Args types. Drops `Void` slots cleanly so
   * `FoldConcat[(Void, Int, Void, String)] = (Int, String)`. Used by variadic builders / projection lists / RETURNING
   * tuples to combine N typed-Args slots into one.
   */
  type FoldConcat[T <: Tuple] = T match {
    case EmptyTuple      => Void
    case h *: EmptyTuple => h
    case h *: t          => Concat[h, FoldConcat[t]]
  }

  /**
   * Project a `FoldConcat[T]` value back into a heterogeneous list of per-slot values, in tuple order — one entry per
   * slot of `T`, including `Void` placeholders for slots whose Args is `Void`. Inline-dispatched on the tuple shape —
   * caller must therefore be `inline` (or have `T` concrete) so the recursion reduces.
   */
  inline def projectFoldConcat[T <: Tuple](c: FoldConcat[T]): List[Any] =
    inline scala.compiletime.erasedValue[T] match
      case _: EmptyTuple        => Nil
      case _: (h *: EmptyTuple) => List(c)
      case _: (h *: t)          =>
        val pair = projectConcat[h, FoldConcat[t & Tuple]](c.asInstanceOf[Concat[h, FoldConcat[t & Tuple]]])
        pair._1 :: projectFoldConcat[t & Tuple](pair._2)

  // ---- Combinators (binop / not) ---------------------------------------------------------------

  private[sharp] def binop[A, B](
    l: TypedExpr[Boolean, A],
    r: TypedExpr[Boolean, B],
    opSql: String,
    proj: Concat[A, B] => (A, B)
  ): TypedExpr[Boolean, Concat[A, B]] = {
    // combineSep's encoder skips the product when a side is `Void.codec`, so `a && b` over static sides stays static.
    apply[Concat[A, B]](TypedExpr.wrap("(", TypedExpr.combineSep[A, B](l.fragment, opSql, r.fragment, proj), ")"))
  }

  private[sharp] def notOf[A](w: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = {
    apply[A](TypedExpr.wrap(NOT_OPEN_KW, w.fragment, ")"))
  }

  /**
   * Adopt a `TypedExpr[Boolean, A]` as a `Where[A]` — identity now that Where is a type alias. Kept for source compat
   * with code that previously called `Where(expr)` to lift a non-Where Boolean expression.
   */
  def fromTypedExpr[A](expr: TypedExpr[Boolean, A]): TypedExpr[Boolean, A] = expr

  // ---- cats-style monoidal folds for Where[Void] ----------------------------------------------
  //
  // `Where[Void] && Where[Void]` reduces to `Where[Void]` (since `Concat[Void, Void] = Void`), which makes a
  // `Monoid[Where[Void]]` lawful for both AND and OR. We don't surface a single auto-discoverable `given`
  // because two monoids on the same type would clash — instead callers either use the helpers below, or
  // explicitly summon one of [[andMonoid]] / [[orMonoid]].
  //
  // Parameterised `Where[A]` (`A != Void`) intentionally has no `Semigroup` — `&&` returns `Where.Concat[A,B]`
  // (a different type), which doesn't match the `(A, A) => A` shape. The Concat machinery exists to *thread*
  // arg slots through the AST; collapsing them via Semigroup would defeat the typed-Args design.

  /** Identity element for [[allOf]] / `andMonoid` — renders as `TRUE` and is optimised away by Postgres. */
  lazy val trueExpr: TypedExpr[Boolean, skunk.Void] = TypedExpr.lit(true)

  /** Identity element for [[anyOf]] / `orMonoid` — renders as `FALSE`. */
  lazy val falseExpr: TypedExpr[Boolean, skunk.Void] = TypedExpr.lit(false)

  /** Monoid combining `Where[Void]`s with `AND`. Empty = `TRUE`. Not a `given` — pick explicitly. */
  val andMonoid: cats.Monoid[TypedExpr[Boolean, skunk.Void]] =
    new cats.Monoid[TypedExpr[Boolean, skunk.Void]] {
      def empty: TypedExpr[Boolean, skunk.Void] = trueExpr
      def combine(
        x: TypedExpr[Boolean, skunk.Void],
        y: TypedExpr[Boolean, skunk.Void]
      ): TypedExpr[Boolean, skunk.Void] = x && y
    }

  /** Monoid combining `Where[Void]`s with `OR`. Empty = `FALSE`. Not a `given` — pick explicitly. */
  val orMonoid: cats.Monoid[TypedExpr[Boolean, skunk.Void]] =
    new cats.Monoid[TypedExpr[Boolean, skunk.Void]] {
      def empty: TypedExpr[Boolean, skunk.Void] = falseExpr
      def combine(
        x: TypedExpr[Boolean, skunk.Void],
        y: TypedExpr[Boolean, skunk.Void]
      ): TypedExpr[Boolean, skunk.Void] = x || y
    }

  /**
   * AND-fold a `Foldable` of `Where[Void]`. Empty input collapses to [[trueExpr]] (`WHERE TRUE`), so callers don't need
   * to special-case the empty list. Non-empty input avoids prepending the identity — the result for `List(a, b, c)` is
   * `(a AND b) AND c`, not `((TRUE AND a) AND b) AND c`.
   */
  def allOf[F[_]: cats.Foldable](xs: F[TypedExpr[Boolean, skunk.Void]]): TypedExpr[Boolean, skunk.Void] =
    cats.Foldable[F].reduceLeftOption(xs)((acc, w) => acc && w).getOrElse(trueExpr)

  /** OR-fold counterpart to [[allOf]]. Empty input collapses to [[falseExpr]] (`WHERE FALSE`). */
  def anyOf[F[_]: cats.Foldable](xs: F[TypedExpr[Boolean, skunk.Void]]): TypedExpr[Boolean, skunk.Void] =
    cats.Foldable[F].reduceLeftOption(xs)((acc, w) => acc || w).getOrElse(falseExpr)

}

/** Combinator extensions on `TypedExpr[Boolean, A]`. */
extension [A](self: TypedExpr[Boolean, A]) {

  /** AND two predicates — combined `Args = Concat[A, B]`. */
  inline def and[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW, Where.projPair[A, B])

  /** Infix AND. */
  inline def &&[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.AND_KW, Where.projPair[A, B])

  /** OR two predicates — combined `Args = Concat[A, B]`. */
  inline def or[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW, Where.projPair[A, B])

  /** Infix OR. */
  inline def ||[B](that: TypedExpr[Boolean, B]): TypedExpr[Boolean, Where.Concat[A, B]] =
    Where.binop(self, that, Where.OR_KW, Where.projPair[A, B])

  /** NOT a predicate. */
  def not: TypedExpr[Boolean, A] = Where.notOf(self)

  /** Unary NOT — same as `.not`. */
  def unary_! : TypedExpr[Boolean, A] = Where.notOf(self)

}
