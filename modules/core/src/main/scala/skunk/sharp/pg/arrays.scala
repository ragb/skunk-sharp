package skunk.sharp.pg

import cats.{Alternative, Foldable}
import cats.syntax.all.*
import skunk.codec.all as pg
import skunk.data.Arr

/**
 * Postgres array support — [[PgTypeFor]] instances for `Arr[T]` over the primitive element types skunk ships codecs for
 * (`int2`, `int4`, `int8`, `numeric`, `float4`, `float8`, `text`) plus a generic instance for any cats `Alternative[F]`
 * — `List[T]`, `Vector[T]`, `Chain[T]`, `LazyList[T]` all work out of the box. Operators (`@>`, `<@`, `&&`, `||`) and
 * functions (`array_length`, `array_append`, `cardinality`, …) live in [[skunk.sharp.pg.functions.PgArray]], mixed into
 * [[skunk.sharp.Pg]].
 *
 * The canonical Scala representation of a Postgres array is skunk's [[skunk.data.Arr]] — one-to-one with the wire
 * format and supports multi-dimensional arrays. Users who prefer a Scala collection in their case classes (`List`,
 * `Vector`, …) get it transparently via the `collPgTypeFor` given.
 *
 * `NonEmptyList` / `NonEmptyVector` etc. intentionally aren't covered — they can't be constructed from an empty array.
 * Users needing non-empty guarantees should decode into `Arr[T]` / `List[T]` and validate at the boundary.
 */
object arrays {

  /**
   * Convert a Postgres array to any cats-`Alternative` sequence (`List`, `Vector`, `Chain`, `LazyList`, …). Multi-
   * dimensional shape flattens to a single-dimensional `F[T]`.
   */
  extension [T](arr: Arr[T]) {

    def to[F[_]](using F: Alternative[F]): F[T] =
      arr.flattenTo(List).foldLeft(F.empty[T])((acc, x) => acc <+> F.pure(x))

  }

  /** Build a one-dimensional Postgres array from any cats-foldable collection. */
  extension [F[_], T](coll: F[T])(using F: Foldable[F]) {
    def toArr: Arr[T] = Arr.fromFoldable(coll)
  }

  // ---- Arr[T] codecs -----------------------------------------------------------------------------

  given arrShortPgTypeFor: PgTypeFor[Arr[Short]]           = PgTypeFor.instance(pg._int2)
  given arrIntPgTypeFor: PgTypeFor[Arr[Int]]               = PgTypeFor.instance(pg._int4)
  given arrLongPgTypeFor: PgTypeFor[Arr[Long]]             = PgTypeFor.instance(pg._int8)
  given arrBigDecimalPgTypeFor: PgTypeFor[Arr[BigDecimal]] = PgTypeFor.instance(pg._numeric)
  given arrFloatPgTypeFor: PgTypeFor[Arr[Float]]           = PgTypeFor.instance(pg._float4)
  given arrDoublePgTypeFor: PgTypeFor[Arr[Double]]         = PgTypeFor.instance(pg._float8)
  given arrStringPgTypeFor: PgTypeFor[Arr[String]]         = PgTypeFor.instance(pg._text)

  // ---- Generic collection codec ------------------------------------------------------------------
  //
  // Any `F[_]` with cats `Foldable` + Scala stdlib `Factory` gets a `PgTypeFor[F[T]]` routing through `Arr[T]`. Reads
  // use `Arr.flattenTo(factory)` to build the target collection; writes go through `Foldable.toList` → `Arr(xs*)`.
  // Multi-dimensional Postgres arrays flatten into a single-dim Scala collection on read.

  given collPgTypeFor[F[_], T](using
    F: Alternative[F],
    Fd: Foldable[F],
    arrFor: PgTypeFor[Arr[T]]
  ): PgTypeFor[F[T]] =
    PgTypeFor.instance(
      arrFor.codec.imap[F[T]](
        _.flattenTo(List).foldLeft(F.empty[T])((acc, x) => acc <+> F.pure(x))
      )(coll => Arr.fromFoldable(coll))
    )

}
