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

  /**
   * Process-wide intern table: maps a SQL string to a single shared `AppliedFragment`. Populated by the
   * `TypedExpr.raw` macro for every compile-time-constant call site, so a `raw("(")` anywhere in the codebase
   * resolves to the same `AppliedFragment` instance regardless of which file it appears in.
   *
   * Bounded by source-code size — only compile-time-known strings flow into this map, so it cannot grow with
   * runtime input. Truly dynamic strings (table aliases, runtime operator symbols, `whereRaw` / `havingRaw`
   * payloads) skip this and go through `rawDynamic`, which always allocates fresh.
   */
  private[sharp] val internTable: java.util.concurrent.ConcurrentHashMap[String, AppliedFragment] =
    new java.util.concurrent.ConcurrentHashMap[String, AppliedFragment]()

  /** Intern a constant string into the cache, returning the cached `AppliedFragment`. Idempotent. */
  private[sharp] def intern(s: String): AppliedFragment = {
    val cached = internTable.get(s)
    if (cached ne null) cached
    else {
      val af = mk(s)
      val prev = internTable.putIfAbsent(s, af)
      if (prev ne null) prev else af
    }
  }

  /** Build a fresh `AppliedFragment` for a runtime-built string — no caching. */
  private[sharp] def rawDynamic(s: String): AppliedFragment = mk(s)

  private def mk(s: String): AppliedFragment = {
    val frag: Fragment[Void] = Fragment(List(Left(s)), Void.codec, Origin.unknown)
    frag(Void)
  }

  // Clause separators — also intern() so `raw("SELECT ")` from any call site resolves to the same instance.
  val SELECT:                AppliedFragment = intern("SELECT ")
  val SELECT_DISTINCT:       AppliedFragment = intern("SELECT DISTINCT ")
  val SELECT_DISTINCT_ON:    AppliedFragment = intern("SELECT DISTINCT ON (")
  val CLOSE_PAREN_SPACE:     AppliedFragment = intern(") ")
  val FROM:                  AppliedFragment = intern(" FROM ")
  val WHERE:                 AppliedFragment = intern(" WHERE ")
  val GROUP_BY:              AppliedFragment = intern(" GROUP BY ")
  val HAVING:                AppliedFragment = intern(" HAVING ")
  val ORDER_BY:              AppliedFragment = intern(" ORDER BY ")
  val ON:                    AppliedFragment = intern(" ON ")
  val AND:                   AppliedFragment = intern(" AND ")
  val VALUES:                AppliedFragment = intern("VALUES ")
  val RETURNING:             AppliedFragment = intern(" RETURNING ")
  val USING:                 AppliedFragment = intern(" USING ")
  val ON_CONFLICT_DO_NOTHING: AppliedFragment = intern(" ON CONFLICT DO NOTHING")

  // Punctuation
  val COMMA_SEP:             AppliedFragment = intern(", ")
  val OPEN_PAREN:            AppliedFragment = intern("(")
  val CLOSE_PAREN:           AppliedFragment = intern(")")

  // ORDER BY direction / null placement keywords
  val ASC:                   AppliedFragment = intern(" ASC")
  val DESC:                  AppliedFragment = intern(" DESC")
  val NULLS_FIRST:           AppliedFragment = intern(" NULLS FIRST")
  val NULLS_LAST:            AppliedFragment = intern(" NULLS LAST")

}
