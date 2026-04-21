package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor

/**
 * Postgres literal constants. Rendered **inline** (no bound parameter) because there's no escape concern, no security
 * benefit, and no semantic gain to parameterising a compile-time-known literal — `ON TRUE` is just a no-op join
 * predicate, not a user-variable value.
 *
 * Intended surface: anywhere a `TypedExpr[Boolean]` / `TypedExpr[Option[T]]` would go. Typical use is a LATERAL JOIN's
 * `.on(_ => Pg.True)` (standard SQL idiom for "JOIN needs an ON but the correlation lives in the inner WHERE");
 * elsewhere users mostly want `col === value`, which goes through `lit` and the usual parameter path.
 */
trait PgConstants {

  /** SQL `TRUE` — rendered inline, not as `$N`. */
  val True: TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("TRUE"), skunk.codec.all.bool)

  /** SQL `FALSE` — rendered inline, not as `$N`. */
  val False: TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("FALSE"), skunk.codec.all.bool)

  /**
   * SQL `NULL` typed as `Option[T]` via the resolved `PgTypeFor[T]`. The codec uses `.opt` so the decoder expects an
   * `Option`. Inline rendering.
   *
   * Not named `Null` to avoid colliding with Scala's `Null` type.
   */
  def nullOf[T](using pf: PgTypeFor[T]): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("NULL"), pf.codec.opt)

}
