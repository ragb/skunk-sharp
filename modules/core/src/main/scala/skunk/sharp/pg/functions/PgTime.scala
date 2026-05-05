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
   * `(aStart, aEnd) OVERLAPS (bStart, bEnd)` — 4 typed positions; Args is the left-fold
   * `Concat[Concat[Concat[A1, A2], A3], A4]`. Custom separator pattern (`, ` inside each pair,
   * `) OVERLAPS (` between pairs) is handled by manually constructing the parts list while still
   * delegating slot dispatch to a per-position projector.
   */
  inline def overlaps[T, A1, A2, A3, A4](
    aStart: TypedExpr[T, A1], aEnd: TypedExpr[T, A2], bStart: TypedExpr[T, A3], bEnd: TypedExpr[T, A4]
  ): Where[Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4]] = {
    type Out  = Where.Concat[Where.Concat[Where.Concat[A1, A2], A3], A4]
    val items = List(aStart.fragment, aEnd.fragment, bStart.fragment, bEnd.fragment)

    val sep        = Left(", "): Either[String, cats.data.State[Int, String]]
    val openParen  = Left("("): Either[String, cats.data.State[Int, String]]
    val pairBreak  = Left(") OVERLAPS ("): Either[String, cats.data.State[Int, String]]
    val closeParen = Left(")"): Either[String, cats.data.State[Int, String]]
    val parts =
      List(openParen) ++ aStart.fragment.parts ++
        List(sep) ++ aEnd.fragment.parts ++
        List(pairBreak) ++ bStart.fragment.parts ++
        List(sep) ++ bEnd.fragment.parts ++
        List(closeParen)

    val enc: skunk.Encoder[Out] = new skunk.Encoder[Out] {
      override val types: List[skunk.data.Type] = items.flatMap(_.encoder.types)
      override val sql: cats.data.State[Int, String] =
        cats.data.State { (n0: Int) =>
          items.zipWithIndex.foldLeft((n0, "")) { case ((n, acc), (f, i)) =>
            val (n1, s) = f.encoder.sql.run(n).value
            val sepStr = i match {
              case 0 => "("
              case 1 => ", "
              case 2 => ") OVERLAPS ("
              case _ => ", "
            }
            (n1, acc + sepStr + s)
          } match { case (n, acc) => (n, acc + ")") }
        }

      override def encode(args: Out): List[Option[skunk.data.Encoded]] = {
        val (a123, a4v) = Where.projectConcat[Where.Concat[Where.Concat[A1, A2], A3], A4](args)
        val (a12, a3v)  = Where.projectConcat[Where.Concat[A1, A2], A3](a123.asInstanceOf[Where.Concat[Where.Concat[A1, A2], A3]])
        val (a1v, a2v)  = Where.projectConcat[A1, A2](a12.asInstanceOf[Where.Concat[A1, A2]])
        val values: List[Any] = List(a1v, a2v, a3v, a4v)
        items.zip(values).flatMap { case (f, v) =>
          val e = f.encoder.asInstanceOf[skunk.Encoder[Any]]
          if (e eq Void.codec) Nil else e.encode(v)
        }
      }
    }
    val frag: Fragment[Out] = Fragment(parts, enc, skunk.util.Origin.unknown)
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

  inline def dateTrunc[T, A](precision: String, e: TypedExpr[T, A])(using pfs: PgTypeFor[String]): TypedExpr[T, A] = {
    val pFrag = Param.bind[String](precision).fragment
    val s1    = TypedExpr.combineSepInl[Void, A](pFrag, ", ", e.fragment).asInstanceOf[Fragment[A]]
    val frag  = TypedExpr.wrap("date_trunc(", s1, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  // -------- Interval arithmetic ----------------------------------------------------------------

  inline def age[T, X, Y](a: TypedExpr[T, X], b: TypedExpr[T, Y]): TypedExpr[Duration, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("age(", inner, ")")
    TypedExpr[Duration, Where.Concat[X, Y]](frag, skunk.codec.all.interval)
  }

  def age[T, A](e: TypedExpr[T, A]): TypedExpr[Duration, A] = unaryOut("age", e, skunk.codec.all.interval)

  def justifyDays[A](e: TypedExpr[Duration, A]):     TypedExpr[Duration, A] = unaryOut("justify_days", e, skunk.codec.all.interval)
  def justifyHours[A](e: TypedExpr[Duration, A]):    TypedExpr[Duration, A] = unaryOut("justify_hours", e, skunk.codec.all.interval)
  def justifyInterval[A](e: TypedExpr[Duration, A]): TypedExpr[Duration, A] = unaryOut("justify_interval", e, skunk.codec.all.interval)

  // -------- Construction -----------------------------------------------------------------------

  /** `make_date(year, month, day)` — 3 typed positions; Args = `Concat[Concat[Y, M], D]`. */
  inline def makeDate[Y, M, D](
    year: TypedExpr[Int, Y], month: TypedExpr[Int, M], day: TypedExpr[Int, D]
  ): TypedExpr[LocalDate, Where.Concat[Where.Concat[Y, M], D]] = {
    val projector: Where.Concat[Where.Concat[Y, M], D] => List[Any] = combined => {
      val (a12, a3v) = Where.projectConcat[Where.Concat[Y, M], D](combined)
      val (a1v, a2v) = Where.projectConcat[Y, M](a12.asInstanceOf[Where.Concat[Y, M]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[Y, M], D]](
      List(year.fragment, month.fragment, day.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("make_date(", combined, ")")
    TypedExpr[LocalDate, Where.Concat[Where.Concat[Y, M], D]](frag, skunk.codec.all.date)
  }

  /** `make_time(h, m, s)` — 3 typed positions; Args = `Concat[Concat[H, M], S]`. */
  inline def makeTime[H, MM, S](
    h: TypedExpr[Int, H], m: TypedExpr[Int, MM], s: TypedExpr[Double, S]
  ): TypedExpr[LocalTime, Where.Concat[Where.Concat[H, MM], S]] = {
    val projector: Where.Concat[Where.Concat[H, MM], S] => List[Any] = combined => {
      val (a12, a3v) = Where.projectConcat[Where.Concat[H, MM], S](combined)
      val (a1v, a2v) = Where.projectConcat[H, MM](a12.asInstanceOf[Where.Concat[H, MM]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[H, MM], S]](
      List(h.fragment, m.fragment, s.fragment), ", ", projector
    )
    val frag = TypedExpr.wrap("make_time(", combined, ")")
    TypedExpr[LocalTime, Where.Concat[Where.Concat[H, MM], S]](frag, skunk.codec.all.time)
  }

  /**
   * `make_timestamp(year, month, day, h, m, s)` — 6 typed positions threaded as
   * `Concat[Concat[Concat[Concat[Concat[Y, MO], D], H], MI], S]` (left-fold).
   */
  inline def makeTimestamp[Y, MO, D, H, MI, S](
    year:  TypedExpr[Int, Y],
    month: TypedExpr[Int, MO],
    day:   TypedExpr[Int, D],
    h:     TypedExpr[Int, H],
    m:     TypedExpr[Int, MI],
    s:     TypedExpr[Double, S]
  ): TypedExpr[LocalDateTime, Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H], MI], S]] = {
    type Out = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H], MI], S]
    val projector: Out => List[Any] = combined => {
      val (a12345, a6v) = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H], MI], S](combined)
      val (a1234, a5v)  = Where.projectConcat[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H], MI](a12345.asInstanceOf[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H], MI]])
      val (a123, a4v)   = Where.projectConcat[Where.Concat[Where.Concat[Y, MO], D], H](a1234.asInstanceOf[Where.Concat[Where.Concat[Where.Concat[Y, MO], D], H]])
      val (a12, a3v)    = Where.projectConcat[Where.Concat[Y, MO], D](a123.asInstanceOf[Where.Concat[Where.Concat[Y, MO], D]])
      val (a1v, a2v)    = Where.projectConcat[Y, MO](a12.asInstanceOf[Where.Concat[Y, MO]])
      List(a1v, a2v, a3v, a4v, a5v, a6v)
    }
    val combined = TypedExpr.combineList[Out](
      List(year.fragment, month.fragment, day.fragment, h.fragment, m.fragment, s.fragment),
      ", ",
      projector
    )
    val frag = TypedExpr.wrap("make_timestamp(", combined, ")")
    TypedExpr[LocalDateTime, Out](frag, skunk.codec.all.timestamp)
  }

  // -------- Parsing ----------------------------------------------------------------------------

  def toTimestamp[A](e: TypedExpr[Double, A]): TypedExpr[OffsetDateTime, A] =
    unaryOut("to_timestamp", e, skunk.codec.all.timestamptz)

  inline def toDate[T, A](e: TypedExpr[T, A], fmt: String)(using ev: StrLike[T], pfs: PgTypeFor[String]): TypedExpr[LocalDate, A] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val s1      = TypedExpr.combineSepInl[A, Void](e.fragment, ", ", fmtFrag).asInstanceOf[Fragment[A]]
    val frag    = TypedExpr.wrap("to_date(", s1, ")")
    TypedExpr[LocalDate, A](frag, skunk.codec.all.date)
  }

  // -------- Helpers -------------------------------------------------------------------------

  private def unaryOut[T, A, R](name: String, e: TypedExpr[T, A], outCodec: Codec[R]): TypedExpr[R, A] = {
    val frag = TypedExpr.wrap(s"$name(", e.fragment, ")")
    TypedExpr[R, A](frag, outCodec)
  }

}
