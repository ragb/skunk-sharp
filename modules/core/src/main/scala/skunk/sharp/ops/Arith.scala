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

/** Result of `L op R` on the operands' non-`Option` types. */
sealed trait ArithOut[L, R] { type Out }

object ArithOut {
  type Aux[L, R, O] = ArithOut[L, R] { type Out = O }
  private val instance: ArithOut[Any, Any] = new ArithOut[Any, Any] {}
  def of[L, R, O]: Aux[L, R, O]            = instance.asInstanceOf[Aux[L, R, O]]

  // ---- Numeric promotion, shared by + - * / ----
  // same type
  given short: ArithOut.Aux[Short, Short, Short]                  = ArithOut.of
  given int: ArithOut.Aux[Int, Int, Int]                          = ArithOut.of
  given long: ArithOut.Aux[Long, Long, Long]                      = ArithOut.of
  given numeric: ArithOut.Aux[BigDecimal, BigDecimal, BigDecimal] = ArithOut.of
  given float: ArithOut.Aux[Float, Float, Float]                  = ArithOut.of
  given double: ArithOut.Aux[Double, Double, Double]              = ArithOut.of
  // integer widening
  given shortInt: ArithOut.Aux[Short, Int, Int]    = ArithOut.of
  given intShort: ArithOut.Aux[Int, Short, Int]    = ArithOut.of
  given shortLong: ArithOut.Aux[Short, Long, Long] = ArithOut.of
  given longShort: ArithOut.Aux[Long, Short, Long] = ArithOut.of
  given intLong: ArithOut.Aux[Int, Long, Long]     = ArithOut.of
  given longInt: ArithOut.Aux[Long, Int, Long]     = ArithOut.of
  // integer ↔ numeric → numeric
  given shortNum: ArithOut.Aux[Short, BigDecimal, BigDecimal] = ArithOut.of
  given numShort: ArithOut.Aux[BigDecimal, Short, BigDecimal] = ArithOut.of
  given intNum: ArithOut.Aux[Int, BigDecimal, BigDecimal]     = ArithOut.of
  given numInt: ArithOut.Aux[BigDecimal, Int, BigDecimal]     = ArithOut.of
  given longNum: ArithOut.Aux[Long, BigDecimal, BigDecimal]   = ArithOut.of
  given numLong: ArithOut.Aux[BigDecimal, Long, BigDecimal]   = ArithOut.of
  // anything ↔ float8 → float8
  given shortDouble: ArithOut.Aux[Short, Double, Double]    = ArithOut.of
  given doubleShort: ArithOut.Aux[Double, Short, Double]    = ArithOut.of
  given intDouble: ArithOut.Aux[Int, Double, Double]        = ArithOut.of
  given doubleInt: ArithOut.Aux[Double, Int, Double]        = ArithOut.of
  given longDouble: ArithOut.Aux[Long, Double, Double]      = ArithOut.of
  given doubleLong: ArithOut.Aux[Double, Long, Double]      = ArithOut.of
  given numDouble: ArithOut.Aux[BigDecimal, Double, Double] = ArithOut.of
  given doubleNum: ArithOut.Aux[Double, BigDecimal, Double] = ArithOut.of
  given floatDouble: ArithOut.Aux[Float, Double, Double]    = ArithOut.of
  given doubleFloat: ArithOut.Aux[Double, Float, Double]    = ArithOut.of
}

/** `L + R`. */
sealed trait Plus[L, R] extends ArithOut[L, R]

object Plus {
  type Aux[L, R, O] = Plus[L, R] { type Out = O }
  private val instance: Plus[Any, Any] = new Plus[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector + vector`). */
  def of[L, R, O]: Aux[L, R, O]                                         = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using ArithOut.Aux[L, R, O]): Aux[L, R, O] = instance.asInstanceOf[Aux[L, R, O]]
  given tstzInterval: Aux[OffsetDateTime, Duration, OffsetDateTime]     = instance.asInstanceOf
  given tsInterval: Aux[LocalDateTime, Duration, LocalDateTime]         = instance.asInstanceOf
  given dateDays: Aux[LocalDate, Int, LocalDate]                        = instance.asInstanceOf
  given dateInterval: Aux[LocalDate, Duration, LocalDateTime]           = instance.asInstanceOf
  given intervals: Aux[Duration, Duration, Duration]                    = instance.asInstanceOf
}

/** `L - R`. */
sealed trait Minus[L, R] extends ArithOut[L, R]

object Minus {
  type Aux[L, R, O] = Minus[L, R] { type Out = O }
  private val instance: Minus[Any, Any] = new Minus[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector - vector`). */
  def of[L, R, O]: Aux[L, R, O]                                         = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using ArithOut.Aux[L, R, O]): Aux[L, R, O] = instance.asInstanceOf[Aux[L, R, O]]
  given tstzInterval: Aux[OffsetDateTime, Duration, OffsetDateTime]     = instance.asInstanceOf
  given tsInterval: Aux[LocalDateTime, Duration, LocalDateTime]         = instance.asInstanceOf
  given dateDays: Aux[LocalDate, Int, LocalDate]                        = instance.asInstanceOf
  given dateInterval: Aux[LocalDate, Duration, LocalDateTime]           = instance.asInstanceOf
  given dates: Aux[LocalDate, LocalDate, Int]                           = instance.asInstanceOf
  given tstzs: Aux[OffsetDateTime, OffsetDateTime, Duration]            = instance.asInstanceOf
  given tss: Aux[LocalDateTime, LocalDateTime, Duration]                = instance.asInstanceOf
  given intervals: Aux[Duration, Duration, Duration]                    = instance.asInstanceOf
}

/** `L * R`. */
sealed trait Times[L, R] extends ArithOut[L, R]

object Times {
  type Aux[L, R, O] = Times[L, R] { type Out = O }
  private val instance: Times[Any, Any] = new Times[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector * vector`). */
  def of[L, R, O]: Aux[L, R, O]                                         = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using ArithOut.Aux[L, R, O]): Aux[L, R, O] = instance.asInstanceOf[Aux[L, R, O]]
  given intervalScale: Aux[Duration, Double, Duration]                  = instance.asInstanceOf
}

/** `L / R` — integer division truncates, as in Postgres. */
sealed trait Div[L, R] extends ArithOut[L, R]

object Div {
  type Aux[L, R, O] = Div[L, R] { type Out = O }
  private val instance: Div[Any, Any] = new Div[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector / vector`). */
  def of[L, R, O]: Aux[L, R, O]                                         = instance.asInstanceOf[Aux[L, R, O]]
  given fromNumeric[L, R, O](using ArithOut.Aux[L, R, O]): Aux[L, R, O] = instance.asInstanceOf[Aux[L, R, O]]
  given intervalScale: Aux[Duration, Double, Duration]                  = instance.asInstanceOf
}

/** `L % R` — integers and `numeric` only (Postgres has no float modulo). */
sealed trait Mod[L, R] extends ArithOut[L, R]

object Mod {
  type Aux[L, R, O] = Mod[L, R] { type Out = O }
  private val instance: Mod[Any, Any] = new Mod[Any, Any] {}

  /** Build an instance — the extension point for types outside core (e.g. pgvector's `vector % vector`). */
  def of[L, R, O]: Aux[L, R, O]                          = instance.asInstanceOf[Aux[L, R, O]]
  given short: Aux[Short, Short, Short]                  = instance.asInstanceOf
  given int: Aux[Int, Int, Int]                          = instance.asInstanceOf
  given long: Aux[Long, Long, Long]                      = instance.asInstanceOf
  given numeric: Aux[BigDecimal, BigDecimal, BigDecimal] = instance.asInstanceOf
  given intLong: Aux[Int, Long, Long]                    = instance.asInstanceOf
  given longInt: Aux[Long, Int, Long]                    = instance.asInstanceOf
  given intNum: Aux[Int, BigDecimal, BigDecimal]         = instance.asInstanceOf
  given numInt: Aux[BigDecimal, Int, BigDecimal]         = instance.asInstanceOf
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
  def unary_-(using @annotation.unused ev: Plus[Stripped[L], Stripped[L]]): TypedExpr[L, A] =
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
