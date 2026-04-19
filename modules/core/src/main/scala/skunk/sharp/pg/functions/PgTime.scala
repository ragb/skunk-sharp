package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}

import java.time.{LocalDate, LocalDateTime, OffsetDateTime}

/** Date / time accessor functions. Mixed into [[skunk.sharp.Pg]]. */
trait PgTime {

  /** `now()` — current transaction timestamp with timezone. */
  val now: TypedExpr[OffsetDateTime] = PgFunction.nullary[OffsetDateTime]("now")

  /** `current_timestamp` — keyword form, no parentheses. */
  val currentTimestamp: TypedExpr[OffsetDateTime] =
    TypedExpr(TypedExpr.raw("current_timestamp"), skunk.codec.all.timestamptz)

  /** `current_date`. */
  val currentDate: TypedExpr[LocalDate] =
    TypedExpr(TypedExpr.raw("current_date"), skunk.codec.all.date)

  /** `localtimestamp`. */
  val localTimestamp: TypedExpr[LocalDateTime] =
    TypedExpr(TypedExpr.raw("localtimestamp"), skunk.codec.all.timestamp)

}
