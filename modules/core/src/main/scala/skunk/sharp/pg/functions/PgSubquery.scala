package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr

/** Subquery-based predicates. Mixed into [[skunk.sharp.Pg]]. */
trait PgSubquery {

  /**
   * `EXISTS (<subquery>)`. The subquery's row type doesn't matter — only existence. Accepts any
   * [[skunk.sharp.dsl.AsSubquery]]-compatible value (compiled query or un-compiled builder).
   *
   * Rendering is deferred — the subquery's `render` thunk is captured by closure and only invoked when the outermost
   * `.compile` walks `TypedExpr.render`. Inner `.compile` calls (on SelectBuilder / ProjectedSelect subqueries) happen
   * at that moment, not at `Pg.exists(…)`-call time.
   */
  def exists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val thunk = ev.render(sub)
    TypedExpr(TypedExpr.raw("EXISTS (") |+| thunk() |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

  /** `NOT EXISTS (<subquery>)`. Same deferred-rendering contract as [[exists]]. */
  def notExists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val thunk = ev.render(sub)
    TypedExpr(TypedExpr.raw("NOT EXISTS (") |+| thunk() |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

}
