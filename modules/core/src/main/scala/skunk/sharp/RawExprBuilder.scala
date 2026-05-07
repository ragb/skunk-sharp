package skunk.sharp

import skunk.{Codec, Fragment}
import skunk.sharp.pg.PgTypeFor

/**
 * Intermediate stage of the [[expr]] string interpolator. The interpolator weaves the literal SQL parts and the `$col`
 * / `$value` interpolations into a single typed `Fragment[Args]`, but does not know the Scala type the resulting
 * expression decodes to. Callers commit a result type via either [[as]] (resolves `PgTypeFor[T]`'s canonical codec) or
 * [[asCodec]] (explicit `Codec[T]` — useful when reusing an input expression's codec, e.g.
 * `expr"lower($e)".asCodec(e.codec)`).
 */
final class RawExprBuilder[Args](val fragment: Fragment[Args]) {

  /** Commit the result type as `T`, resolving the codec via `PgTypeFor[T]`. */
  def as[T](using pf: PgTypeFor[T]): TypedExpr[T, Args] =
    TypedExpr(fragment, pf.codec)

  /** Commit the result type as `T` with an explicit `Codec[T]`. */
  def asCodec[T](codec: Codec[T]): TypedExpr[T, Args] =
    TypedExpr(fragment, codec)

  /**
   * Commit the result type as `T`, reusing an existing `TypedExpr`'s codec. The common shape for tag- / type-preserving
   * functions where the result codec is the input expression's codec — replaces `.asCodec(e.codec)` with `.asCodec(e)`.
   */
  def asCodec[T](e: TypedExpr[T, ?]): TypedExpr[T, Args] =
    TypedExpr(fragment, e.codec)

}
