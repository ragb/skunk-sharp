package skunk.sharp.where

import skunk.sharp.TypedExpr

/**
 * A WHERE-clause predicate — just a `TypedExpr[Boolean]`. Used to live as a thin wrapper class; collapsed into a type
 * alias so any expression producing a boolean value (user operators, `Pg.exists`, subqueries, extension-module
 * predicates) can be used directly without a `Where(...)` wrap at call sites.
 *
 * All chaining methods (`&&`, `and`, `||`, `or`, unary `!`, `not`) live as extension methods on `TypedExpr[Boolean]` —
 * see [[WhereOps]] below.
 */
type Where = TypedExpr[Boolean]

object Where {

  /**
   * Identity helper kept for source compatibility with call sites that read more naturally as `Where(Pg.exists(sub))`.
   * `Where(e)` now just is `e`, so prefer passing `TypedExpr[Boolean]` directly.
   */
  inline def apply(expr: TypedExpr[Boolean]): Where = expr

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

  private def lift(af: skunk.AppliedFragment): Where =
    new TypedExpr[Boolean] {
      val render = af
      val codec  = skunk.codec.all.bool
    }

}

/**
 * Chaining ops on any `TypedExpr[Boolean]` (= `Where`) — `&&` / `and`, `||` / `or`, unary `!` / `not`. Because `Where`
 * is now a type alias, these are equally available on any boolean expression: `Pg.exists(sub) && u.age >= 18`.
 */
extension (lhs: Where) {
  def &&(rhs: Where): Where  = Where.and(lhs, rhs)
  def and(rhs: Where): Where = Where.and(lhs, rhs)
  def ||(rhs: Where): Where  = Where.or(lhs, rhs)
  def or(rhs: Where): Where  = Where.or(lhs, rhs)
  def unary_! : Where        = Where.not(lhs)
  def not: Where             = Where.not(lhs)
}
