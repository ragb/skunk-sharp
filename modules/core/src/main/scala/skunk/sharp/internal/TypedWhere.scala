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
private[sharp] final case class TypedWhere[Args](
  fragment: Fragment[Args],
  args:     Args
) {
  def codec: Codec[Boolean] = skunk.codec.all.bool

  /** Escape hatch back to the existential Where — useful during migration so mixed call sites keep compiling. */
  def toWhere: Where = TypedExpr(fragment(args), codec)
}
