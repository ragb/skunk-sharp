package skunk.sharp.data

/**
 * A Postgres range value parameterized by the element type.
 *
 * Construct via the companion:
 * {{{
 *   Range(lower = Some(1), upper = Some(10))           // [1,10)
 *   Range(lower = Some(1), upper = Some(10), lowerInclusive = false) // (1,10)
 *   Range(upper = Some(10))                            // (,10)  unbounded lower
 *   Range.empty                                        // empty range
 * }}}
 *
 * When used as the element type inside [[skunk.sharp.pg.tags.PgRange]], the codec translates to the corresponding
 * Postgres range literal on the wire (e.g. `[1,10)`, `["2020-01-01","2021-01-01")`, `empty`).
 */
sealed trait Range[+A]

object Range {

  /** An empty range — contains no values. `isempty(r)` returns `true` for this. */
  case object Empty extends Range[Nothing]

  /**
   * A non-empty range with optional lower/upper bounds.
   *
   * `None` for `lower` / `upper` means unbounded (−∞ / +∞ respectively). Postgres renders unbounded bounds with an
   * empty string inside the range literal (`[,10)`).
   *
   * `lowerInclusive = true` → `[` (default); `false` → `(`. `upperInclusive = false` → `)` (default for non-discrete
   * types); `true` → `]`.
   *
   * Note: Postgres canonicalises discrete range types (int4, int8, date) to `[lower, upper)` on read-back, so the
   * decoded form may differ from what was written.
   */
  final case class Bounds[+A](
    lower: Option[A],
    upper: Option[A],
    lowerInclusive: Boolean,
    upperInclusive: Boolean
  ) extends Range[A]

  /** The canonical empty range. */
  def empty[A]: Range[A] = Empty

  /**
   * Convenience constructor for a `Bounds` range. Defaults: lower-inclusive `[`, upper-exclusive `)`. Pass `None` for
   * an unbounded endpoint.
   */
  def apply[A](
    lower: Option[A] = None,
    upper: Option[A] = None,
    lowerInclusive: Boolean = true,
    upperInclusive: Boolean = false
  ): Range[A] = Bounds(lower, upper, lowerInclusive, upperInclusive)

}
