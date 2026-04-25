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

  /**
   * Process-wide counter of fresh `AppliedFragment` allocations driven by runtime-built SQL strings —
   * `s"…"` interpolations against variables, `whereRaw` / `havingRaw` payloads. Read by benchmarks /
   * diagnostics to track progress toward the "everything except whereRaw/havingRaw is static" goal.
   * The counter is best-effort — concurrent compiles may race on increments, but the order-of-magnitude
   * count over millions of compiles is what matters.
   */
  private[sharp] val rawDynamicCount: java.util.concurrent.atomic.AtomicLong =
    new java.util.concurrent.atomic.AtomicLong(0L)

  /**
   * When set to a non-null map, every `rawDynamic` call records the calling stack frame as a key and
   * increments its counter. Used by benchmarks to attribute the per-compile dynamic-AF count to specific
   * call sites. Off by default (null) so production has zero overhead.
   */
  @volatile private[sharp] var rawDynamicByCallSite: java.util.concurrent.ConcurrentHashMap[String, Long] = null

  /**
   * Bounded array caches for `" LIMIT n"` and `" OFFSET n"` AppliedFragments. Pagination patterns reuse a
   * small fixed set of values (10, 20, 25, 50, 100, …); pre-allocating the first `LimitOffsetCacheSize`
   * entries lazily covers the realistic spread without unbounded memory growth. Larger values fall back to
   * `rawDynamic` and allocate fresh per compile.
   */
  private final val LimitOffsetCacheSize = 1024
  private val limitCache:  Array[AppliedFragment] = new Array[AppliedFragment](LimitOffsetCacheSize)
  private val offsetCache: Array[AppliedFragment] = new Array[AppliedFragment](LimitOffsetCacheSize)

  /** Resolve `" LIMIT n"` to a cached AF when `0 <= n < 1024`, otherwise allocate fresh via `rawDynamic`. */
  private[sharp] def limitAf(n: Int): AppliedFragment = {
    if (n >= 0 && n < LimitOffsetCacheSize) {
      val cached = limitCache(n)
      if (cached ne null) cached
      else {
        val af = mk(s" LIMIT $n")
        limitCache(n) = af
        af
      }
    } else rawDynamic(s" LIMIT $n")
  }

  /** Resolve `" OFFSET n"` to a cached AF when `0 <= n < 1024`, otherwise allocate fresh via `rawDynamic`. */
  private[sharp] def offsetAf(n: Int): AppliedFragment = {
    if (n >= 0 && n < LimitOffsetCacheSize) {
      val cached = offsetCache(n)
      if (cached ne null) cached
      else {
        val af = mk(s" OFFSET $n")
        offsetCache(n) = af
        af
      }
    } else rawDynamic(s" OFFSET $n")
  }

  /** Build a fresh `AppliedFragment` for a runtime-built string — no caching. */
  private[sharp] def rawDynamic(s: String): AppliedFragment = {
    rawDynamicCount.incrementAndGet()
    val cs = rawDynamicByCallSite
    if (cs ne null) {
      val frames = Thread.currentThread.getStackTrace
      // Skip the first 3 frames: getStackTrace, rawDynamic, the macro splice site.
      val key =
        if (frames.length > 3) {
          val f = frames(3)
          s"${f.getClassName}.${f.getMethodName}(${f.getFileName}:${f.getLineNumber})"
        } else "<unknown>"
      val _ = cs.merge(key, 1L, (a, b) => a + b)
    }
    mk(s)
  }

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
