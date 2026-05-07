package skunk.sharp

import skunk.{Codec, Fragment}
import skunk.sharp.pg.PgTypeFor
import skunk.util.Origin

/**
 * A typed parameter placeholder — declares "a value of type `T` will be supplied at execute time" without binding an
 * actual value at builder-build time. `Param[T] extends TypedExpr[T, T]`, so it slots wherever the DSL accepts a typed
 * expression — WHERE, HAVING, ORDER BY, LIMIT/OFFSET, SET RHS, INSERT VALUES, JOIN ON, function args, CASE branches,
 * projections — and contributes `T` to the surrounding expression's `Args` type.
 *
 * Used to construct **static queries** that live as top-level vals (companion objects, repository fields) before any
 * user request arrives:
 *
 * {{{
 *   val byId: QueryTemplate[UUID, User] =
 *     users.select.where(u => u.id === Param[UUID]).compile
 *
 *   prep <- byId.prepared(session)
 *   user <- prep.unique(realId)
 * }}}
 *
 * For compile-time primitive constants use [[TypedExpr.lit]]. For runtime values the user already has in hand and wants
 * baked into a Void-args fragment (rare — mostly for migration / `whereRaw` interop), see [[Param.bind]].
 */
final class Param[T](val pcodec: Codec[T]) extends TypedExpr[T, T] {

  val codec: Codec[T] = pcodec

  /** The placeholder fragment: a single `Right` part with the codec. Encoder is the parameter's codec. */
  val fragment: Fragment[T] = Fragment(List(Right(pcodec.sql)), pcodec, Origin.unknown)

}

object Param {

  /**
   * Construct a `Param[T]` resolving the codec from `PgTypeFor[T]`. The summoner picks the canonical Postgres type for
   * `T` (e.g. `Param[UUID]` → `uuid`, `Param[Int]` → `int4`). For an explicit codec, use [[Param.of]].
   */
  def apply[T](using pf: PgTypeFor[T]): Param[T] = new Param[T](pf.codec)

  /** Construct a `Param[T]` from an explicit `Codec[T]`. Use when the canonical `PgTypeFor[T]` codec doesn't fit. */
  def of[T](codec: Codec[T]): Param[T] = new Param[T](codec)

  /**
   * A `Param[List[T]]` for a fixed-size list — useful for `IN` lists where you want **one** prepared statement per size
   * and a single `List[T]` bind at execute time, instead of N separate `Param.bind`s baked at builder-build time.
   *
   * {{{
   *   val byIds = users.select.where(u => u.id.in(Param.list[UUID](3))).compile
   *   //                                                  ^ size known at compile time
   *   // val _: QueryTemplate[List[UUID], User] = byIds
   *   byIds.run(session)(List(uid1, uid2, uid3))
   * }}}
   *
   * The result fragment expands to N comma-separated `$N` placeholders sharing one prepared statement. Bind a list of
   * exactly `size` elements at execute time — skunk raises if the list length disagrees.
   *
   * For lists whose size varies between calls, either rebuild the query per size (different prepared statement each
   * time) or reach for `col === ANY(Param[Arr[T]])` (single statement, single array bind).
   */
  def list[T](size: Int)(using pf: PgTypeFor[T]): Param[List[T]] = {
    val inner                 = pf.codec
    val enc                   = inner.list(size)
    val codec: Codec[List[T]] = new Codec[List[T]] {
      override def encode(xs: List[T]): List[Option[skunk.data.Encoded]]                               = enc.encode(xs)
      override def decode(offset: Int, ss: List[Option[String]]): Either[skunk.Decoder.Error, List[T]] =
        Left(skunk.Decoder.Error(
          offset,
          size,
          "Param.list[T] is bind-only — IN-list parameters can't appear in a SELECT projection."
        ))
      override val types: List[skunk.data.Type]      = enc.types
      override val sql: cats.data.State[Int, String] = enc.sql
    }
    Param.of(codec)
  }

  /**
   * Bake a runtime value into a `TypedExpr[T, Void]` — the value is fixed at construction time, not supplied at
   * execute. This is what the value-taking operator overloads (`=== v`, `>= v`, …) call internally; you rarely need it
   * directly unless you are constructing a dynamic expression outside the built-in operators.
   *
   * Prefer [[Param]] without `.bind` for static queries — that lets the user supply args at execute time and preserves
   * Skunk's plan-cache friendliness.
   */
  def bind[T](value: T)(using pf: PgTypeFor[T]): TypedExpr[T, skunk.Void] =
    TypedExpr.parameterised[T](value)(using pf)

}
