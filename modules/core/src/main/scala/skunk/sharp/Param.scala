package skunk.sharp

import skunk.{Codec, Fragment}
import skunk.sharp.pg.PgTypeFor
import skunk.util.Origin

/**
 * A typed parameter placeholder — declares "a value of type `T` will be supplied at execute time" without binding
 * an actual value at builder-build time. `Param[T] extends TypedExpr[T, T]`, so it slots wherever the DSL accepts
 * a typed expression — WHERE, HAVING, ORDER BY, LIMIT/OFFSET, SET RHS, INSERT VALUES, JOIN ON, function args,
 * CASE branches, projections — and contributes `T` to the surrounding expression's `Args` type.
 *
 * Used to construct **static queries** that live as top-level vals (companion objects, repository fields) before
 * any user request arrives:
 *
 * {{{
 *   val byId: QueryTemplate[UUID, User] =
 *     users.select.where(u => u.id === Param[UUID]).compile
 *
 *   prep <- byId.prepared(session)
 *   user <- prep.unique(realId)
 * }}}
 *
 * For compile-time primitive constants use [[TypedExpr.lit]]. For runtime values the user already has in hand and
 * wants baked into a Void-args fragment (rare — mostly for migration / `whereRaw` interop), see [[Param.bind]].
 */
final class Param[T] (val pcodec: Codec[T]) extends TypedExpr[T, T] {

  val codec: Codec[T] = pcodec

  /** The placeholder fragment: a single `Right` part with the codec. Encoder is the parameter's codec. */
  val fragment: Fragment[T] = Fragment(List(Right(pcodec.sql)), pcodec, Origin.unknown)

}

object Param {

  /**
   * Construct a `Param[T]` resolving the codec from `PgTypeFor[T]`. The summoner picks the canonical Postgres type
   * for `T` (e.g. `Param[UUID]` → `uuid`, `Param[Int]` → `int4`). For an explicit codec, use [[Param.of]].
   */
  def apply[T](using pf: PgTypeFor[T]): Param[T] = new Param[T](pf.codec)

  /** Construct a `Param[T]` from an explicit `Codec[T]`. Use when the canonical `PgTypeFor[T]` codec doesn't fit. */
  def of[T](codec: Codec[T]): Param[T] = new Param[T](codec)

  /**
   * Bake a runtime value into a `TypedExpr[T, Void]` — the value is fixed at construction time, not supplied at
   * execute. Equivalent to today's captured-args path, but explicit. Mostly useful when:
   *
   *   - building dynamic AppliedFragments programmatically and you want a typed-expression handle on a specific
   *     value,
   *   - migration aid where existing code wrote `=== runtimeValue` — wrap as `=== Param.bind(value)` to preserve
   *     behavior verbatim.
   *
   * Prefer [[Param]] without `.bind` for static queries — that lets the user supply args at execute time and
   * preserves Skunk's plan-cache friendliness.
   */
  def bind[T](value: T)(using pf: PgTypeFor[T]): TypedExpr[T, skunk.Void] =
    TypedExpr.parameterised[T](value)(using pf)

}
