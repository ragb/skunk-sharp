package skunk.sharp.pg.functions

import skunk.{Codec, Fragment, Void}
import skunk.sharp.{Param, PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

/** Date / time accessor functions and keyword constants. Args of input expression(s) propagate. */
trait PgTime {

  val now: TypedExpr[OffsetDateTime, Void] = PgFunction.nullary[OffsetDateTime]("now")

  val currentTimestamp: TypedExpr[OffsetDateTime, Void] = TypedExpr(TypedExpr.voidFragment("current_timestamp"), skunk.codec.all.timestamptz)
  val currentDate:      TypedExpr[LocalDate, Void]      = TypedExpr(TypedExpr.voidFragment("current_date"),      skunk.codec.all.date)
  val currentTime:      TypedExpr[OffsetTime, Void]     = TypedExpr(TypedExpr.voidFragment("current_time"),      skunk.codec.all.timetz)
  val localTimestamp:   TypedExpr[LocalDateTime, Void]  = TypedExpr(TypedExpr.voidFragment("localtimestamp"),    skunk.codec.all.timestamp)
  val localTime:        TypedExpr[LocalTime, Void]      = TypedExpr(TypedExpr.voidFragment("localtime"),         skunk.codec.all.time)

  /**
   * `(aStart, aEnd) OVERLAPS (bStart, bEnd)`. The four inputs' Args thread via nested `Where.Concat` —
   * collapses to `Void` when all inputs are Void (typical column-only use), or to a typed tuple when
   * `Param[T]` arms are present.
   */
  def overlaps[T, A1, A2, A3, A4](
    aStart: TypedExpr[T, A1], aEnd: TypedExpr[T, A2], bStart: TypedExpr[T, A3], bEnd: TypedExpr[T, A4]
  ): Where[Where.Concat[Where.Concat[A1, A2], Where.Concat[A3, A4]]] = {
    val s1   = TypedExpr.combineSep(aStart.fragment, ", ", aEnd.fragment)
    val s2   = TypedExpr.combineSep(bStart.fragment, ", ", bEnd.fragment)
    val mid  = TypedExpr.combineSep(s1, ") OVERLAPS (", s2)
    val frag = TypedExpr.wrap("(", mid, ")")
    Where(frag)
  }

  // -------- Field extraction -------------------------------------------------------------------

  def extract[T, A](field: String, e: TypedExpr[T, A])(using
    pf: PgTypeFor[Lift[T, BigDecimal]]
  ): TypedExpr[Lift[T, BigDecimal], A] = {
    val parts = List[Either[String, cats.data.State[Int, String]]](Left(s"extract($field FROM ")) ++ e.fragment.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val frag = Fragment[A](parts, e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[Lift[T, BigDecimal], A](frag, pf.codec)
  }

  // -------- Truncation -------------------------------------------------------------------------

  def dateTrunc[T, A](precision: String, e: TypedExpr[T, A])(using pfs: PgTypeFor[String]): TypedExpr[T, A] = {
    val pFrag = Param.bind[String](precision).fragment
    val s1    = TypedExpr.combineSep(pFrag, ", ", e.fragment).asInstanceOf[Fragment[A]]
    val frag  = TypedExpr.wrap("date_trunc(", s1, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  // -------- Interval arithmetic ----------------------------------------------------------------

  def age[T, X, Y](a: TypedExpr[T, X], b: TypedExpr[T, Y]): TypedExpr[Duration, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("age(", inner, ")")
    TypedExpr[Duration, Where.Concat[X, Y]](frag, skunk.codec.all.interval)
  }

  def age[T, A](e: TypedExpr[T, A]): TypedExpr[Duration, A] = unaryOut("age", e, skunk.codec.all.interval)

  def justifyDays[A](e: TypedExpr[Duration, A]):     TypedExpr[Duration, A] = unaryOut("justify_days", e, skunk.codec.all.interval)
  def justifyHours[A](e: TypedExpr[Duration, A]):    TypedExpr[Duration, A] = unaryOut("justify_hours", e, skunk.codec.all.interval)
  def justifyInterval[A](e: TypedExpr[Duration, A]): TypedExpr[Duration, A] = unaryOut("justify_interval", e, skunk.codec.all.interval)

  // -------- Construction -----------------------------------------------------------------------

  def makeDate(year: TypedExpr[Int, ?], month: TypedExpr[Int, ?], day: TypedExpr[Int, ?]): TypedExpr[LocalDate, Void] = {
    val joined = TypedExpr.joinedVoid(", ", List(year.fragment, month.fragment, day.fragment))
    val frag   = TypedExpr.wrap("make_date(", joined, ")")
    TypedExpr[LocalDate, Void](frag, skunk.codec.all.date)
  }

  def makeTime(h: TypedExpr[Int, ?], m: TypedExpr[Int, ?], s: TypedExpr[Double, ?]): TypedExpr[LocalTime, Void] = {
    val joined = TypedExpr.joinedVoid(", ", List(h.fragment, m.fragment, s.fragment))
    val frag   = TypedExpr.wrap("make_time(", joined, ")")
    TypedExpr[LocalTime, Void](frag, skunk.codec.all.time)
  }

  def makeTimestamp(
    year: TypedExpr[Int, ?], month: TypedExpr[Int, ?], day: TypedExpr[Int, ?],
    h: TypedExpr[Int, ?], m: TypedExpr[Int, ?], s: TypedExpr[Double, ?]
  ): TypedExpr[LocalDateTime, Void] = {
    val joined = TypedExpr.joinedVoid(", ",
      List(year.fragment, month.fragment, day.fragment, h.fragment, m.fragment, s.fragment))
    val frag   = TypedExpr.wrap("make_timestamp(", joined, ")")
    TypedExpr[LocalDateTime, Void](frag, skunk.codec.all.timestamp)
  }

  // -------- Parsing ----------------------------------------------------------------------------

  def toTimestamp[A](e: TypedExpr[Double, A]): TypedExpr[OffsetDateTime, A] =
    unaryOut("to_timestamp", e, skunk.codec.all.timestamptz)

  def toDate[T, A](e: TypedExpr[T, A], fmt: String)(using ev: StrLike[T], pfs: PgTypeFor[String]): TypedExpr[LocalDate, A] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val s1      = TypedExpr.combineSep(e.fragment, ", ", fmtFrag).asInstanceOf[Fragment[A]]
    val frag    = TypedExpr.wrap("to_date(", s1, ")")
    TypedExpr[LocalDate, A](frag, skunk.codec.all.date)
  }

  // -------- Helpers -------------------------------------------------------------------------

  private def unaryOut[T, A, R](name: String, e: TypedExpr[T, A], outCodec: Codec[R]): TypedExpr[R, A] = {
    val frag = TypedExpr.wrap(s"$name(", e.fragment, ")")
    TypedExpr[R, A](frag, outCodec)
  }

}
