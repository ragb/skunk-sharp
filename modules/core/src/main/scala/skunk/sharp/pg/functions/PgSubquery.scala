package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr

/** Subquery-based predicates. Mixed into [[skunk.sharp.Pg]]. */
trait PgSubquery {

  /**
   * `EXISTS (<subquery>)`. The subquery's row type doesn't matter — only existence. Accepts any
   * [[skunk.sharp.dsl.AsSubquery]]-compatible value (compiled query or un-compiled builder).
   */
  def exists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

  /** `NOT EXISTS (<subquery>)`. */
  def notExists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("NOT EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

}
