package skunk.sharp.ops

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import java.time.{Duration, LocalDate, LocalDateTime, OffsetDateTime}

/*
 * Infix arithmetic on expressions: `+ - * / %` and unary `-`, each rendered parenthesised — `("age" + $1)` — so any
 * composition renders correctly regardless of SQL operator precedence.
 *
 * The result type follows Postgres's own typing, via one typeclass per operator (`Plus`, `Minus`, `Times`, `Div`,
 * `Mod`) over the operands' non-`Option` types, then lifted to `Option` when either side is nullable:
 *
 *   - numbers: integers widen (`int2 < int4 < int8`); an integer with `numeric` gives `numeric`; anything with
 *     `float8` gives `float8`; `float4` pairs with `float4`. `int / int` truncates, as in Postgres. `%` is integers and
 *     `numeric` only.
 *   - time: `timestamptz`/`timestamp` ± `interval`; `date` ± `int` (days); `date` ± `interval` → `timestamp`;
 *     `date - date` → `int` (days); `timestamptz - timestamptz` and `timestamp - timestamp` → `interval`; `interval ±
 *     interval`, `interval * float8`.
 *
 * Other types (text, uuid, …) have no instance, so `email + 1` doesn't compile. Extension types ship their own
 * instances in their companion via `Plus.of` / `Minus.of` / … (pgvector's `vector ± vector` does).
 */

/*
 * Each operator is its own sealed typeclass with its own instances — no shared supertrait, so an instance for one
 * operator (e.g. pgvector's `vector + vector`) never counts as evidence for another (`vector / vector` stays a clean
 * "no instance" error). The shared tables below (`NumericPromotion`, `AdditiveTime`) are separate typeclasses that the
 * operators *derive* from, never extend.
 */

/** Postgres's result type for `L op R` over numbers, shared by `+ - * /`. */
sealed trait NumericPromotion[L, R] { type Out }

object NumericPromotion {
  type Aux[L, R, O] = NumericPromotion[L, R] { type Out = O }
  private val instance: NumericPromotion[Any, Any] = new NumericPromotion[Any, Any] {}
  private def of[L, R, O]: Aux[L, R, O]            = instance.asInstanceOf[Aux[L, R, O]]

  // ---- Numeric promotion, shared by + - * / ----
  // same type
  given short: Aux[Short, Short, Short]                  = of
  given int: Aux[Int, Int, Int]                          = of
  given long: Aux[Long, Long, Long]                      = of
  given numeric: Aux[BigDecimal, BigDecimal, BigDecimal] = of
  given float: Aux[Float, Float, Float]                  = of
  given double: Aux[Double, Double, Double]              = of
  // integer widening
  given shortInt: Aux[Short, Int, Int]    = of
  given intShort: Aux[Int, Short, Int]    = of
  given shortLong: Aux[Short, Long, Long] = of
  given longShort: Aux[Long, Short, Long] = of
  given intLong: Aux[Int, Long, Long]     = of
  given longInt: Aux[Long, Int, Long]     = of
  // integer ↔ numeric → numeric
  given shortNum: Aux[Short, BigDecimal, BigDecimal] = of
  given numShort: Aux[BigDecimal, Short, BigDecimal] = of
  given intNum: Aux[Int, BigDecimal, BigDecimal]     = of
  given numInt: Aux[BigDecimal, Int, BigDecimal]     = of
  given longNum: Aux[Long, BigDecimal, BigDecimal]   = of
  given numLong: Aux[BigDecimal, Long, BigDecimal]   = of
  // anything ↔ float8 → float8
  given shortDouble: Aux[Short, Double, Double]    = of
  given doubleShort: Aux[Double, Short, Double]    = of
  given intDouble: Aux[Int, Double, Double]        = of
  given doubleInt: Aux[Double, Int, Double]        = of
  given longDouble: Aux[Long, Double, Double]      = of
  given doubleLong: Aux[Double, Long, Double]      = of
  given numDouble: Aux[BigDecimal, Double, Double] = of
  given doubleNum: Aux[Double, BigDecimal, Double] = of
  given floatDouble: Aux[Float, Double, Double]    = of
  given doubleFloat: Aux[Double, Float, Double]    = of
}

/**
 * Date / time shifts shared by `+` and `-`: `timestamptz`/`timestamp` ± interval, `date` ± days / interval, interval ±
 * interval.
 */
sealed trait AdditiveTime[L, R] { type Out }

object AdditiveTime {
  type Aux[L, R, O] = AdditiveTime[L, R] { type Out = O }
  private val instance: AdditiveTime[Any, Any] = new AdditiveTime[Any, Any] {}
  private def of[L, R, O]: Aux[L, R, O]        = instance.asInstanceOf[Aux[L, R, O]]

  given tstzInterval: Aux[OffsetDateTime, Duration, OffsetDateTime] = of
  given tsInterval: Aux[LocalDateTime, Duration, LocalDateTime]     = of
  given dateDays: Aux[LocalDate, Int, LocalDate]                    = of
  given dateInterval: Aux[LocalDate, Duration, LocalDateTime]       = of
  given intervals: Aux[Duration, Duration, Duration]                = of
}

/** `L + R`. */
sealed trait Plus[L, R] { type Out }

object Plus {
  type Aux[L, R, O] = Plus[L, R] { type Out = O }
  private val instance: Plus[Any, Any] = new Plus[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector + vector`). */
  def of[L, R, O]: Aux[L, R, O]                                                 = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using NumericPromotion.Aux[L, R, O]): Aux[L, R, O] = of
  given fromTime[L, R, O](using AdditiveTime.Aux[L, R, O]): Aux[L, R, O]        = of
}

/** `L - R`. */
sealed trait Minus[L, R] { type Out }

object Minus {
  type Aux[L, R, O] = Minus[L, R] { type Out = O }
  private val instance: Minus[Any, Any] = new Minus[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector - vector`). */
  def of[L, R, O]: Aux[L, R, O]                                                 = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using NumericPromotion.Aux[L, R, O]): Aux[L, R, O] = of
  given fromTime[L, R, O](using AdditiveTime.Aux[L, R, O]): Aux[L, R, O]        = of
  given dates: Aux[LocalDate, LocalDate, Int]                                   = of
  given tstzs: Aux[OffsetDateTime, OffsetDateTime, Duration]                    = of
  given tss: Aux[LocalDateTime, LocalDateTime, Duration]                        = of
}

/** `L * R`. */
sealed trait Times[L, R] { type Out }

object Times {
  type Aux[L, R, O] = Times[L, R] { type Out = O }
  private val instance: Times[Any, Any] = new Times[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector * vector`). */
  def of[L, R, O]: Aux[L, R, O]                                                 = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using NumericPromotion.Aux[L, R, O]): Aux[L, R, O] = of
  given intervalScale: Aux[Duration, Double, Duration]                          = of
}

/** `L / R` — integer division truncates, as in Postgres. */
sealed trait Div[L, R] { type Out }

object Div {
  type Aux[L, R, O] = Div[L, R] { type Out = O }
  private val instance: Div[Any, Any] = new Div[Any, Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[L, R, O]: Aux[L, R, O]                                                 = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using NumericPromotion.Aux[L, R, O]): Aux[L, R, O] = of
  given intervalScale: Aux[Duration, Double, Duration]                          = of
}

/** `L % R` — integers and `numeric` only (Postgres has no float modulo). */
sealed trait Mod[L, R] { type Out }

object Mod {
  type Aux[L, R, O] = Mod[L, R] { type Out = O }
  private val instance: Mod[Any, Any] = new Mod[Any, Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[L, R, O]: Aux[L, R, O]                          = instance.asInstanceOf[Aux[L, R, O]]
  given short: Aux[Short, Short, Short]                  = of
  given int: Aux[Int, Int, Int]                          = of
  given long: Aux[Long, Long, Long]                      = of
  given numeric: Aux[BigDecimal, BigDecimal, BigDecimal] = of
  given shortInt: Aux[Short, Int, Int]                   = of
  given intShort: Aux[Int, Short, Int]                   = of
  given shortLong: Aux[Short, Long, Long]                = of
  given longShort: Aux[Long, Short, Long]                = of
  given intLong: Aux[Int, Long, Long]                    = of
  given longInt: Aux[Long, Int, Long]                    = of
  given shortNum: Aux[Short, BigDecimal, BigDecimal]     = of
  given numShort: Aux[BigDecimal, Short, BigDecimal]     = of
  given intNum: Aux[Int, BigDecimal, BigDecimal]         = of
  given numInt: Aux[BigDecimal, Int, BigDecimal]         = of
  given longNum: Aux[Long, BigDecimal, BigDecimal]       = of
  given numLong: Aux[BigDecimal, Long, BigDecimal]       = of
}

/** Unary `-`: numbers and intervals. Extension types opt in with `Negate.of` (pgvector has no prefix `-`). */
sealed trait Negate[T]

object Negate {
  private val instance: Negate[Any] = new Negate[Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[T]: Negate[T]              = instance.asInstanceOf[Negate[T]]
  given short: Negate[Short]        = of
  given int: Negate[Int]            = of
  given long: Negate[Long]          = of
  given numeric: Negate[BigDecimal] = of
  given float: Negate[Float]        = of
  given double: Negate[Double]      = of
  given interval: Negate[Duration]  = of
}

/** `Option[O]` when either operand is nullable, else `O`. */
type ArithResult[L, R, O] = L match {
  case Option[?] => Option[O]
  case _         =>
    R match {
      case Option[?] => Option[O]
      case _         => O
    }
}

extension [L, A](lhs: TypedExpr[L, A]) {

  /** `(lhs + rhs)`. */
  inline def +[R, B, O](rhs: TypedExpr[R, B])(using
    op: Plus.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[ArithResult[L, R, O]]
  ): TypedExpr[ArithResult[L, R, O], Where.Concat[A, B]] =
    Arith.binary[L, R, ArithResult[L, R, O], A, B](" + ", lhs, rhs)

  /** `(lhs - rhs)`. */
  inline def -[R, B, O](rhs: TypedExpr[R, B])(using
    op: Minus.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[ArithResult[L, R, O]]
  ): TypedExpr[ArithResult[L, R, O], Where.Concat[A, B]] =
    Arith.binary[L, R, ArithResult[L, R, O], A, B](" - ", lhs, rhs)

  /** `(lhs * rhs)`. */
  inline def *[R, B, O](rhs: TypedExpr[R, B])(using
    op: Times.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[ArithResult[L, R, O]]
  ): TypedExpr[ArithResult[L, R, O], Where.Concat[A, B]] =
    Arith.binary[L, R, ArithResult[L, R, O], A, B](" * ", lhs, rhs)

  /** `(lhs / rhs)` — integer division truncates. */
  inline def /[R, B, O](rhs: TypedExpr[R, B])(using
    op: Div.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[ArithResult[L, R, O]]
  ): TypedExpr[ArithResult[L, R, O], Where.Concat[A, B]] =
    Arith.binary[L, R, ArithResult[L, R, O], A, B](" / ", lhs, rhs)

  /** `(lhs % rhs)` — integers and `numeric`. */
  inline def %[R, B, O](rhs: TypedExpr[R, B])(using
    op: Mod.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[ArithResult[L, R, O]]
  ): TypedExpr[ArithResult[L, R, O], Where.Concat[A, B]] =
    Arith.binary[L, R, ArithResult[L, R, O], A, B](" % ", lhs, rhs)

  /** `(- lhs)` — numbers and intervals. */
  def unary_-(using @annotation.unused ev: Negate[Stripped[L]]): TypedExpr[L, A] =
    TypedExpr[L, A](TypedExpr.wrap("(- ", lhs.fragment, ")"), lhs.codec)

}

object Arith {

  /** `(lhs op rhs)`, parenthesised so it composes under any surrounding operator. */
  inline def binary[L, R, O, A, B](op: String, lhs: TypedExpr[L, A], rhs: TypedExpr[R, B])(using
    pf: PgTypeFor[O]
  ): TypedExpr[O, Where.Concat[A, B]] = {
    val inner = TypedExpr.combineSepInl[A, B](lhs.fragment, op, rhs.fragment)
    TypedExpr[O, Where.Concat[A, B]](TypedExpr.wrap("(", inner, ")"), pf.codec)
  }

}
