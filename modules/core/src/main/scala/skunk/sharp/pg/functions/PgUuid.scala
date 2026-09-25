package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.{PgFunction, TypedExpr}

import java.time.{Duration, OffsetDateTime}
import java.util.UUID

/**
 * UUID generation and inspection. `uuidv4` / `uuidv7` / `uuidExtract*` need Postgres 18+; `genRandomUuid` works on 13+.
 */
trait PgUuid {

  private val genRandomUuidExpr: TypedExpr[UUID, Void] = PgFunction.nullary[UUID]("gen_random_uuid")
  private val uuidv4Expr: TypedExpr[UUID, Void]        = PgFunction.nullary[UUID]("uuidv4")
  private val uuidv7Expr: TypedExpr[UUID, Void]        = PgFunction.nullary[UUID]("uuidv7")

  /** `gen_random_uuid()` — random (version 4) UUID. */
  def genRandomUuid: TypedExpr[UUID, Void] = genRandomUuidExpr

  /** `uuidv4()` — random (version 4) UUID; PG 18 alias of `gen_random_uuid()`. */
  def uuidv4: TypedExpr[UUID, Void] = uuidv4Expr

  /** `uuidv7()` — time-ordered (version 7, RFC 9562) UUID. Sorts by creation time, so it suits primary keys. */
  def uuidv7: TypedExpr[UUID, Void] = uuidv7Expr

  /** `uuidv7(shift)` — version 7 UUID whose embedded timestamp is the current time shifted by `shift`. */
  def uuidv7[A](shift: TypedExpr[Duration, A]): TypedExpr[UUID, A] =
    TypedExpr[UUID, A](TypedExpr.wrap("uuidv7(", shift.fragment, ")"), skunk.codec.all.uuid)

  /**
   * `uuid_extract_timestamp(u)` — the timestamp embedded in a version 1 or 7 UUID. Always `Option`: Postgres returns
   * NULL for any other version.
   */
  def uuidExtractTimestamp[T, A](u: TypedExpr[T, A])(using UuidLike[T]): TypedExpr[Option[OffsetDateTime], A] =
    TypedExpr[Option[OffsetDateTime], A](
      TypedExpr.wrap("uuid_extract_timestamp(", u.fragment, ")"),
      skunk.codec.all.timestamptz.opt
    )

  /**
   * `uuid_extract_version(u)` — the UUID's version number. Always `Option`: Postgres returns NULL for UUIDs that are
   * not of the RFC 9562 variant.
   */
  def uuidExtractVersion[T, A](u: TypedExpr[T, A])(using UuidLike[T]): TypedExpr[Option[Short], A] =
    TypedExpr[Option[Short], A](TypedExpr.wrap("uuid_extract_version(", u.fragment, ")"), skunk.codec.all.int2.opt)

}
