package skunk.sharp.internal

import skunk.{Codec, Fragment}
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * A `Where` (= `TypedExpr[Boolean]`) whose argument-tuple type is visible at the call site.
 *
 * Today's `Where` / `TypedExpr[Boolean]` hide the bound-parameter tuple inside an opaque
 * `AppliedFragment` (via `.render`) — once you cross the `AppliedFragment` boundary the caller can't tell a single-
 * arg predicate (`u.age >= 18`) from a two-arg predicate (`u.age.between(18, 65)`). `TypedWhere[Args]` surfaces
 * that tuple as a type parameter so a `.whereTyped` / `.compileTyped` pipeline can accumulate Args across the
 * builder chain and deliver `CompiledQuery[Args, R]` with a concrete `Args` at the final call site.
 *
 * Only a narrow slice of the DSL produces `TypedWhere[_]` today — see `SqlMacros.infixTyped` and the routes that
 * plug into it. Other operators (`&&`, `||`, `between`, `in(...)`, subquery forms) still produce the existential
 * `Where` and must be incrementally lifted.
 */
final class TypedWhere[Args](
  val fragment: Fragment[Args],
  val args:     Args
) extends TypedExpr[Boolean] {

  val codec: Codec[Boolean] = skunk.codec.all.bool

  /** Render to an `AppliedFragment`, losing the `Args` type. Satisfies the `TypedExpr` contract. */
  lazy val render: skunk.AppliedFragment = fragment(args)

  /** Back-compat alias — now redundant (this instance IS a Where) but kept for call sites that used it. */
  def toWhere: Where = this

  /**
   * AND two typed predicates, combining their argument tuples. The resulting `Args` is the pair `(Args, That)` —
   * skunk's twiddle-pair style matches how [[skunk.Fragment.~]] composes. Callers typically chain via the
   * `TypedWhere.&&` extension for infix syntax.
   */
  def and[That](that: TypedWhere[That]): TypedWhere[(Args, That)] = {
    import skunk.sharp.internal.RawConstants
    // Parts: ( Args-frag  AND  That-frag )
    // The outer parens match Where.and's runtime form — parenthesised AND for unambiguous grouping.
    val parts =
      RawConstants.OPEN_PAREN.fragment.parts ++
        fragment.parts ++
        RawConstants.AND.fragment.parts ++
        that.fragment.parts ++
        RawConstants.CLOSE_PAREN.fragment.parts

    // Combined encoder: product of the two. skunk's Encoder has `.product` which gives Encoder[(A, B)].
    val combinedEncoder = fragment.encoder.product(that.fragment.encoder)

    val combinedFragment: Fragment[(Args, That)] =
      Fragment(parts, combinedEncoder, skunk.util.Origin.unknown)

    TypedWhere[(Args, That)](combinedFragment, (args, that.args))
  }
}

object TypedWhere {

  def apply[Args](fragment: Fragment[Args], args: Args): TypedWhere[Args] =
    new TypedWhere[Args](fragment, args)

  /** Infix AND — `p1 && p2` — delegating to [[TypedWhere.and]]. */
  extension [A](lhs: TypedWhere[A]) def &&[B](rhs: TypedWhere[B]): TypedWhere[(A, B)] = lhs.and(rhs)

}
