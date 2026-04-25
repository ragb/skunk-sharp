package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/** Subquery-based predicates. Mixed into [[skunk.sharp.Pg]]. */
trait PgSubquery {

  /**
   * `EXISTS (<subquery>)`. The subquery's row type doesn't matter — only existence. Accepts any
   * [[skunk.sharp.dsl.AsSubquery]]-compatible value. Returned as `Where[skunk.Void]` — the subquery's bound
   * parameters are baked into the underlying AppliedFragment via `Where.fromTypedExpr`.
   */
  def exists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): Where[skunk.Void] = {
    val thunk = ev.render(sub)
    Where(TypedExpr(TypedExpr.raw("EXISTS (") |+| thunk() |+| TypedExpr.raw(")"), skunk.codec.all.bool))
  }

  /** `NOT EXISTS (<subquery>)`. */
  def notExists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): Where[skunk.Void] = {
    val thunk = ev.render(sub)
    Where(TypedExpr(TypedExpr.raw("NOT EXISTS (") |+| thunk() |+| TypedExpr.raw(")"), skunk.codec.all.bool))
  }

}
