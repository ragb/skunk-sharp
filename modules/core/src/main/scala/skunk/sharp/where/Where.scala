package skunk.sharp.where

import skunk.{Codec, Fragment, Void}
import skunk.sharp.TypedExpr
import skunk.sharp.internal.RawConstants
import skunk.util.Origin

/**
 * A WHERE-clause predicate carrying its captured-parameter tuple as a visible type parameter.
 *
 * `Args` is the tuple of values bound by `$N` placeholders in `fragment`. For a leaf predicate `u.age >= 18`,
 * `Args = Int` and `args = 18`. For combined predicates `(u.age >= 18) && (u.email === "x")`, `Args = (Int, String)`
 * — left-associated raw pairs, no normalisation. For parameterless predicates (`col.isNull`), `Args = Void`.
 *
 * Extends `TypedExpr[Boolean]` so `Where[A]` slots wherever a boolean expression is expected (SELECT projections,
 * `Pg.exists` arms, function args, …). The `render: AppliedFragment` derived view is `fragment(args)` — used by
 * code paths that don't track `Args` (composing into AppliedFragment-shaped APIs like ORDER BY, projections).
 *
 * Combinators `&&`, `and`, `||`, `or`, `unary_!`, `not` thread `Args` via tuple pairing — `Where[A] && Where[B]`
 * is `Where[(A, B)]`. The `&&` chain mirrors skunk's `~` twiddle pair shape, so `Encoder.product` composes
 * straightforwardly under the hood.
 */
final class Where[Args] private[sharp] (
  val fragment: Fragment[Args],
  val args:     Args
) extends TypedExpr[Boolean] {

  val codec: Codec[Boolean] = skunk.codec.all.bool

  /** Project to an `AppliedFragment`, losing the `Args` type. Used by code paths that work in AppliedFragment-land. */
  lazy val render: skunk.AppliedFragment = fragment(args)

  /** AND two predicates — combined `Args = (A, B)`. */
  def and[B](that: Where[B]): Where[(Args, B)] = Where.binop(this, that, RawConstants.AND)

  /** Infix AND. */
  def &&[B](that: Where[B]): Where[(Args, B)] = and(that)

  /** OR two predicates. */
  def or[B](that: Where[B]): Where[(Args, B)] = Where.binop(this, that, Where.OR_KW)

  /** Infix OR. */
  def ||[B](that: Where[B]): Where[(Args, B)] = or(that)

  /** NOT a predicate. */
  def not: Where[Args] = Where.notOf(this)

  /** Unary NOT — same as `.not`. */
  def unary_! : Where[Args] = not
}

object Where {

  private[sharp] val OR_KW:  skunk.AppliedFragment = TypedExpr.raw(" OR ")
  private[sharp] val NOT_KW: skunk.AppliedFragment = TypedExpr.raw("NOT (")

  /** Construct directly from a typed Fragment + args. */
  def apply[A](fragment: Fragment[A], args: A): Where[A] = new Where[A](fragment, args)

  /**
   * Adopt a `TypedExpr[Boolean]` as a `Where[Void]`. Convenience for third-party operators (`Pg.exists(sub)`,
   * range / array boolean ops) that build their predicate as a `TypedExpr[Boolean]` directly. Same effect as
   * `fromTypedExpr` but readable as a constructor at the call site.
   */
  def apply(expr: TypedExpr[Boolean]): Where[Void] = fromTypedExpr(expr)

  /**
   * Adopt an arbitrary `TypedExpr[Boolean]` as a `Where[?]`. Used when wrapping a subquery-derived predicate
   * (`Pg.exists(sub)`) where the inner args are existential — we lift to `Where[skunk.Void]` by treating the
   * pre-applied `AppliedFragment` as a void-args fragment with the bound values baked in via `Encoder.contramap`.
   */
  def fromTypedExpr(expr: TypedExpr[Boolean]): Where[Void] = expr match {
    case w: Where[?] @unchecked => w.asInstanceOf[Where[Void]] // identity-ish path; reuses existing Where.
    case other =>
      // Pre-applied: take the AppliedFragment and bake its args into a Void-typed fragment via contramap.
      val af = other.render
      val srcEnc: skunk.Encoder[Any] = af.fragment.encoder.asInstanceOf[skunk.Encoder[Any]]
      val srcArgs: Any               = af.argument
      val voidEnc: skunk.Encoder[Void] = srcEnc.contramap[Void](_ => srcArgs)
      val frag: Fragment[Void]         = Fragment(af.fragment.parts, voidEnc, Origin.unknown)
      new Where[Void](frag, Void)
  }

  // ---- Combinators (binop / not) ---------------------------------------------------------------

  private def binop[A, B](l: Where[A], r: Where[B], op: skunk.AppliedFragment): Where[(A, B)] = {
    val parts =
      RawConstants.OPEN_PAREN.fragment.parts ++
        l.fragment.parts ++
        op.fragment.parts ++
        r.fragment.parts ++
        RawConstants.CLOSE_PAREN.fragment.parts
    val enc                       = l.fragment.encoder.product(r.fragment.encoder)
    val frag: Fragment[(A, B)]    = Fragment(parts, enc, Origin.unknown)
    new Where[(A, B)](frag, (l.args, r.args))
  }

  private def notOf[A](w: Where[A]): Where[A] = {
    val parts =
      NOT_KW.fragment.parts ++ w.fragment.parts ++ RawConstants.CLOSE_PAREN.fragment.parts
    val frag: Fragment[A] = Fragment(parts, w.fragment.encoder, Origin.unknown)
    new Where[A](frag, w.args)
  }

  // ---- Args composition: "no args yet" sentinel + concat ----------------------------------------

  /**
   * Type-level concat: drop `Void` placeholders so `(Void, A)` collapses to `A`, `(A, Void)` to `A`, and
   * `(Void, Void)` to `Void`. Used by builders to keep their `Args` parameter clean when one of WHERE / HAVING /
   * SET / VALUES is empty.
   */
  type Concat[A, B] = (A, B) match {
    case (Void, Void) => Void
    case (Void, b)    => b
    case (a, Void)    => a
    case _            => (A, B)
  }

  /** Runtime counterpart of [[Concat]] — pair two args, normalising `Void` placeholders away. */
  private[sharp] def concatArgs(a: Any, b: Any): Any = (a, b) match {
    case (Void, Void) => Void
    case (Void, x)    => x
    case (x, Void)    => x
    case (x, y)       => (x, y)
  }

  /**
   * Runtime counterpart of [[Concat]] for skunk encoders — pair two encoders, normalising `Void` away. Returns an
   * `Encoder[Any]` because the result type depends on the [[Concat]] reduction; callers cast at the
   * Fragment-construction site where the typed `Args` shape is known.
   */
  private[sharp] def concatEncoders(a: skunk.Encoder[?], b: skunk.Encoder[?]): skunk.Encoder[?] =
    if (a eq Void.codec) b
    else if (b eq Void.codec) a
    else a.asInstanceOf[skunk.Encoder[Any]].product(b.asInstanceOf[skunk.Encoder[Any]])

}
