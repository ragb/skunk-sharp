package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/** Subquery-based predicates. Args of the inner subquery propagates via `AsSubquery`. */
trait PgSubquery {

  /** `EXISTS (<subquery>)`. */
  def exists[Q, T, A](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T, A]): Where[A] = {
    val inner = ev.fragment(sub)
    val frag  = TypedExpr.wrap("EXISTS (", inner, ")")
    Where(frag)
  }

  /** `NOT EXISTS (<subquery>)`. */
  def notExists[Q, T, A](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T, A]): Where[A] = {
    val inner = ev.fragment(sub)
    val frag  = TypedExpr.wrap("NOT EXISTS (", inner, ")")
    Where(frag)
  }

}
