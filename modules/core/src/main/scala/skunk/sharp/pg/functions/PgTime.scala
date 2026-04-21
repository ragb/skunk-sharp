package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

/** Date / time accessor functions and keyword constants. Mixed into [[skunk.sharp.Pg]]. */
trait PgTime {

  /** `now()` — current transaction timestamp with timezone. */
  val now: TypedExpr[OffsetDateTime] = PgFunction.nullary[OffsetDateTime]("now")

  /** `current_timestamp` — keyword form, no parentheses. */
  val currentTimestamp: TypedExpr[OffsetDateTime] =
    TypedExpr(TypedExpr.raw("current_timestamp"), skunk.codec.all.timestamptz)

  /** `current_date`. */
  val currentDate: TypedExpr[LocalDate] =
    TypedExpr(TypedExpr.raw("current_date"), skunk.codec.all.date)

  /** `current_time` — current time with timezone. */
  val currentTime: TypedExpr[OffsetTime] =
    TypedExpr(TypedExpr.raw("current_time"), skunk.codec.all.timetz)

  /** `localtimestamp` — current timestamp without timezone. */
  val localTimestamp: TypedExpr[LocalDateTime] =
    TypedExpr(TypedExpr.raw("localtimestamp"), skunk.codec.all.timestamp)

  /** `localtime` — current time without timezone. */
  val localTime: TypedExpr[LocalTime] =
    TypedExpr(TypedExpr.raw("localtime"), skunk.codec.all.time)

}
