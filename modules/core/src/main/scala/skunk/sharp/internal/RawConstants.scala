package skunk.sharp.internal

import skunk.{AppliedFragment, Fragment, Void}
import skunk.util.Origin

/**
 * Cached `AppliedFragment` values for SQL tokens that appear in every compiled query — keywords, separators,
 * parentheses. Each `TypedExpr.raw(s)` otherwise allocates a fresh `Fragment` + `AppliedFragment` per call,
 * and a single `.compile` can walk through a dozen of them (`" WHERE "`, `" AND "`, `", "`, `" FROM "`, …).
 *
 * These are process-wide singletons. Safe to share across all threads — `AppliedFragment` is immutable and
 * carries no state beyond the (interned) SQL string and the argument list.
 *
 * Constructed via direct `Fragment` / `AppliedFragment` calls — deliberately does NOT route through
 * `TypedExpr.raw`, which now looks up these constants and would circularly initialise as `null`.
 */
private[sharp] object RawConstants {

  private def mk(s: String): AppliedFragment = {
    val frag: Fragment[Void] = Fragment(List(Left(s)), Void.codec, Origin.unknown)
    frag(Void)
  }

  // Clause separators
  val SELECT:                AppliedFragment = mk("SELECT ")
  val SELECT_DISTINCT:       AppliedFragment = mk("SELECT DISTINCT ")
  val SELECT_DISTINCT_ON:    AppliedFragment = mk("SELECT DISTINCT ON (")
  val CLOSE_PAREN_SPACE:     AppliedFragment = mk(") ")
  val FROM:                  AppliedFragment = mk(" FROM ")
  val WHERE:                 AppliedFragment = mk(" WHERE ")
  val GROUP_BY:              AppliedFragment = mk(" GROUP BY ")
  val HAVING:                AppliedFragment = mk(" HAVING ")
  val ORDER_BY:              AppliedFragment = mk(" ORDER BY ")
  val ON:                    AppliedFragment = mk(" ON ")
  val AND:                   AppliedFragment = mk(" AND ")
  val VALUES:                AppliedFragment = mk("VALUES ")
  val RETURNING:             AppliedFragment = mk(" RETURNING ")
  val USING:                 AppliedFragment = mk(" USING ")
  val ON_CONFLICT_DO_NOTHING: AppliedFragment = mk(" ON CONFLICT DO NOTHING")

  // Punctuation
  val COMMA_SEP:             AppliedFragment = mk(", ")
  val OPEN_PAREN:            AppliedFragment = mk("(")
  val CLOSE_PAREN:           AppliedFragment = mk(")")

}
