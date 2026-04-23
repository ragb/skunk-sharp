package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

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

  /**
   * `(aStart, aEnd) OVERLAPS (bStart, bEnd)` — true if the two time intervals share at least an instant. Postgres
   * accepts date, time, timestamp, and timestamptz pairs — we don't type-gate `T` here because OVERLAPS also applies to
   * intervals and any two comparable "point" types Postgres knows; the server raises a clear type error on misuse.
   * Either endpoint may be NULL (treated as unbounded on that side).
   */
  def overlaps[T](
    aStart: TypedExpr[T],
    aEnd: TypedExpr[T],
    bStart: TypedExpr[T],
    bEnd: TypedExpr[T]
  ): skunk.sharp.where.Where =
    new TypedExpr[Boolean] {
      val render =
        TypedExpr.raw("(") |+| aStart.render |+|
          TypedExpr.raw(", ") |+| aEnd.render |+|
          TypedExpr.raw(") OVERLAPS (") |+| bStart.render |+|
          TypedExpr.raw(", ") |+| bEnd.render |+|
          TypedExpr.raw(")")
      val codec = skunk.codec.all.bool
    }

  // -------- Field extraction -------------------------------------------------------------------

  /**
   * `extract(field FROM e)` — extract a date/time field as `BigDecimal` (Postgres 14+ returns `numeric`). `field` is a
   * bare SQL keyword such as `"year"`, `"month"`, `"epoch"`. Tracks input nullability via [[Lift]].
   */
  def extract[T](field: String, e: TypedExpr[T])(using
    pf: PgTypeFor[Lift[T, BigDecimal]]
  ): TypedExpr[Lift[T, BigDecimal]] =
    TypedExpr(TypedExpr.raw(s"extract($field FROM ") |+| e.render |+| TypedExpr.raw(")"), pf.codec)

  // -------- Truncation -------------------------------------------------------------------------

  /** `date_trunc(precision, e)` — truncate to the given precision (e.g. `"month"`); preserves the input type. */
  def dateTrunc[T](precision: String, e: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("date_trunc(") |+| TypedExpr.parameterised(precision).render |+|
        TypedExpr.raw(", ") |+| e.render |+| TypedExpr.raw(")"),
      e.codec
    )

  // -------- Interval arithmetic ----------------------------------------------------------------

  /** `age(a, b)` — interval between two timestamps. */
  def age[T](a: TypedExpr[T], b: TypedExpr[T]): TypedExpr[Duration] =
    TypedExpr(
      TypedExpr.raw("age(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      skunk.codec.all.interval
    )

  /** `age(ts)` — interval between `ts` and `current_date` (at midnight). */
  def age[T](e: TypedExpr[T]): TypedExpr[Duration] =
    TypedExpr(TypedExpr.raw("age(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.interval)

  /** `justify_days(interval)` — convert days ≥ 30 into months. */
  def justifyDays(e: TypedExpr[Duration]): TypedExpr[Duration] =
    TypedExpr(TypedExpr.raw("justify_days(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.interval)

  /** `justify_hours(interval)` — convert hours ≥ 24 into days. */
  def justifyHours(e: TypedExpr[Duration]): TypedExpr[Duration] =
    TypedExpr(TypedExpr.raw("justify_hours(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.interval)

  /** `justify_interval(interval)` — apply both [[justifyDays]] and [[justifyHours]]. */
  def justifyInterval(e: TypedExpr[Duration]): TypedExpr[Duration] =
    TypedExpr(TypedExpr.raw("justify_interval(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.interval)

  // -------- Construction -----------------------------------------------------------------------

  /** `make_date(year, month, day)`. */
  def makeDate(year: TypedExpr[Int], month: TypedExpr[Int], day: TypedExpr[Int]): TypedExpr[LocalDate] =
    TypedExpr(
      TypedExpr.raw("make_date(") |+| year.render |+| TypedExpr.raw(", ") |+|
        month.render |+| TypedExpr.raw(", ") |+| day.render |+| TypedExpr.raw(")"),
      skunk.codec.all.date
    )

  /** `make_time(h, m, s)` — `s` accepts fractional seconds. */
  def makeTime(h: TypedExpr[Int], m: TypedExpr[Int], s: TypedExpr[Double]): TypedExpr[LocalTime] =
    TypedExpr(
      TypedExpr.raw("make_time(") |+| h.render |+| TypedExpr.raw(", ") |+|
        m.render |+| TypedExpr.raw(", ") |+| s.render |+| TypedExpr.raw(")"),
      skunk.codec.all.time
    )

  /** `make_timestamp(year, month, day, h, m, s)` — timestamp without time zone. */
  def makeTimestamp(
    year: TypedExpr[Int],
    month: TypedExpr[Int],
    day: TypedExpr[Int],
    h: TypedExpr[Int],
    m: TypedExpr[Int],
    s: TypedExpr[Double]
  ): TypedExpr[LocalDateTime] =
    TypedExpr(
      TypedExpr.raw("make_timestamp(") |+|
        year.render |+| TypedExpr.raw(", ") |+| month.render |+| TypedExpr.raw(", ") |+|
        day.render |+| TypedExpr.raw(", ") |+| h.render |+| TypedExpr.raw(", ") |+|
        m.render |+| TypedExpr.raw(", ") |+| s.render |+| TypedExpr.raw(")"),
      skunk.codec.all.timestamp
    )

  // -------- Parsing ----------------------------------------------------------------------------

  /** `to_timestamp(d)` — convert Unix epoch seconds (`double precision`) to `timestamptz`. */
  def toTimestamp(e: TypedExpr[Double]): TypedExpr[OffsetDateTime] =
    TypedExpr(TypedExpr.raw("to_timestamp(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.timestamptz)

  /** `to_date(s, fmt)` — parse a date string using the Postgres `fmt` picture. */
  def toDate[T](e: TypedExpr[T], fmt: String)(using StrLike[T]): TypedExpr[LocalDate] =
    TypedExpr(
      TypedExpr.raw("to_date(") |+| e.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.parameterised(fmt).render |+| TypedExpr.raw(")"),
      skunk.codec.all.date
    )

}
