package skunk.sharp.where

import skunk.AppliedFragment
import skunk.sharp.TypedExpr

/**
 * A WHERE-clause predicate. Thin wrapper around a `TypedExpr[Boolean]` so the compiler stays open to extension: any
 * third-party operator that produces a `TypedExpr[Boolean]` slots in as a `Where` via `.toWhere`.
 *
 * Not a sealed ADT on purpose — the compiler never pattern-matches on variants, it just calls `render`.
 */
final class Where private[where] (val expr: TypedExpr[Boolean]) {

  def render: AppliedFragment = expr.render

  def &&(other: Where): Where  = Where.and(this, other)
  def and(other: Where): Where = Where.and(this, other)

  def ||(other: Where): Where = Where.or(this, other)
  def or(other: Where): Where = Where.or(this, other)

  def unary_! : Where = Where.not(this)
}

object Where {

  /** Lift a boolean expression into a Where predicate. */
  def apply(expr: TypedExpr[Boolean]): Where = new Where(expr)

  /** AND two predicates. */
  def and(l: Where, r: Where): Where = {
    val rendered = TypedExpr.raw("(") |+| l.render |+| TypedExpr.raw(" AND ") |+| r.render |+| TypedExpr.raw(")")
    lift(rendered)
  }

  /** OR two predicates. */
  def or(l: Where, r: Where): Where = {
    val rendered = TypedExpr.raw("(") |+| l.render |+| TypedExpr.raw(" OR ") |+| r.render |+| TypedExpr.raw(")")
    lift(rendered)
  }

  /** NOT a predicate. */
  def not(w: Where): Where = {
    val rendered = TypedExpr.raw("NOT (") |+| w.render |+| TypedExpr.raw(")")
    lift(rendered)
  }

  private def lift(af: AppliedFragment): Where =
    new Where(new TypedExpr[Boolean] {
      val render = af
      val codec  = skunk.codec.all.bool
    })

}
