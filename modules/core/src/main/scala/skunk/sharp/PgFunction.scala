package skunk.sharp

import skunk.sharp.pg.PgTypeFor

/**
 * Typed constructors for Postgres functions and operators.
 *
 * These are the extension hooks third-party modules and user code lean on to expose typed SQL functions:
 *
 *   - `nullary` — a zero-argument function (`now()`, `current_date`). Produces a `TypedExpr[R]` directly.
 *   - `unary` — a one-argument function (`lower(x)`, `length(x)`). Returns `TypedExpr[A] => TypedExpr[R]`.
 *   - `binary` — a two-argument function (`greatest(a, b)`, …). Returns a 2-arg callable.
 *   - `nary` — an n-argument function (`concat(a, b, c)`). Takes a varargs `TypedExpr[?]*`.
 *
 * The `PgOperator` sibling covers infix operators (`a op b`) — same shape, SQL syntax differs.
 */
object PgFunction {

  /** A zero-argument function. The SQL rendering is `name()`. */
  def nullary[R](name: String)(using pfr: PgTypeFor[R]): TypedExpr[R] =
    TypedExpr(TypedExpr.raw(s"$name()"), pfr.codec)

  /** A one-argument function: `name(arg)`. */
  def unary[A, R](name: String)(using pfr: PgTypeFor[R]): TypedExpr[A] => TypedExpr[R] =
    arg => TypedExpr(TypedExpr.raw(s"$name(") |+| arg.render |+| TypedExpr.raw(")"), pfr.codec)

  /** A two-argument function: `name(a, b)`. */
  def binary[A, B, R](name: String)(using pfr: PgTypeFor[R]): (TypedExpr[A], TypedExpr[B]) => TypedExpr[R] =
    (a, b) =>
      TypedExpr(
        TypedExpr.raw(s"$name(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
        pfr.codec
      )

  /**
   * An n-argument function: `name(a, b, c, …)`. At least one argument is required at runtime, but the signature does
   * not enforce that — Postgres will reject empty calls anyway.
   */
  def nary[R](name: String, args: TypedExpr[?]*)(using pfr: PgTypeFor[R]): TypedExpr[R] = {
    val rendered =
      TypedExpr.raw(s"$name(") |+| TypedExpr.joined(args.toList.map(_.render), ", ") |+| TypedExpr.raw(")")
    TypedExpr(rendered, pfr.codec)
  }

}

/**
 * Typed constructors for Postgres infix operators. Third-party modules (jsonb `->>`, ltree `~`, …) use this to expose
 * operator extensions without touching core.
 */
object PgOperator {

  /** An infix binary operator: `a op b`. */
  def infix[A, B, R](op: String)(using pfr: PgTypeFor[R]): (TypedExpr[A], TypedExpr[B]) => TypedExpr[R] =
    (a, b) => TypedExpr(a.render |+| TypedExpr.raw(s" $op ") |+| b.render, pfr.codec)

  /** A prefix unary operator: `op a`. */
  def prefix[A, R](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A] => TypedExpr[R] =
    a => TypedExpr(TypedExpr.raw(s"$op") |+| a.render, pfr.codec)

  /** A postfix unary operator: `a op`. */
  def postfix[A, R](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A] => TypedExpr[R] =
    a => TypedExpr(a.render |+| TypedExpr.raw(s" $op"), pfr.codec)

}

/**
 * Shorthand: evidence that `T` (possibly wrapped in `Option`) is a string-like type — covers `String`, all string tag
 * types (`Varchar[N]`, `Bpchar[N]`, `Text`), and their `Option` variants. Used as an implicit constraint on
 * string-taking [[Pg]] functions so callers retain tag information through the call.
 */
type StrLike[T] = skunk.sharp.where.Stripped[T] <:< String

/**
 * A small set of built-in Postgres functions provided out of the box. Users write `Pg.now`, `Pg.lower(col)`, etc.
 * Extension modules are expected to add their own namespaces (`Jsonb.`, `Ltree.`, …) following the same pattern.
 */
object Pg {

  import java.time.{LocalDate, LocalDateTime, OffsetDateTime}

  /** `now()` — current transaction timestamp with timezone. */
  val now: TypedExpr[OffsetDateTime] = PgFunction.nullary[OffsetDateTime]("now")

  /** `current_timestamp` — keyword, no parentheses. */
  val currentTimestamp: TypedExpr[OffsetDateTime] =
    TypedExpr(TypedExpr.raw("current_timestamp"), skunk.codec.all.timestamptz)

  /** `current_date`. */
  val currentDate: TypedExpr[LocalDate] =
    TypedExpr(TypedExpr.raw("current_date"), skunk.codec.all.date)

  /** `localtimestamp`. */
  val localTimestamp: TypedExpr[LocalDateTime] =
    TypedExpr(TypedExpr.raw("localtimestamp"), skunk.codec.all.timestamp)

  /**
   * `lower(s)` — fold to lowercase. Accepts any string-like `TypedExpr` (bare `String`, tag types like `Varchar[N]` /
   * `Bpchar[N]` / `Text`, or their `Option` variants — anything where `Stripped[T] <:< String`). Returns the input type
   * back: tag information is preserved in the Scala type (PG returns `text` on the wire, but `Varchar[N]` / `Text` etc.
   * all decode the same text bytes through the same codec, so the value is correct).
   */
  def lower[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("lower", e)

  /** `upper(s)` — fold to uppercase. Shape mirrors [[lower]]. */
  def upper[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("upper", e)

  /**
   * `length(s)` — character count. Output nullability tracks the input: `TypedExpr[String]` → `TypedExpr[Int]`;
   * `TypedExpr[Option[Varchar[N]]]` → `TypedExpr[Option[Int]]` (PG's strict null propagation).
   */
  def length[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("length", e)

  /** `concat(a, b, c, …)` — string concatenation. Each arg may carry its own string-like tag. */
  def concat(args: TypedExpr[String]*): TypedExpr[String] =
    PgFunction.nary[String]("concat", args*)

  /** Coalesce: `coalesce(a, b, c, …)` — returns the first non-null argument. */
  def coalesce[T](args: TypedExpr[T]*)(using pfr: PgTypeFor[T]): TypedExpr[T] =
    PgFunction.nary[T]("coalesce", args*)

  // -------- Math (same type as input — codec is threaded through) --------
  //
  // Postgres is permissive about what numeric types these accept (smallint / integer / bigint / numeric / real /
  // double precision). We don't bound `T` — if you pass a non-numeric `TypedExpr`, Postgres raises the error at
  // execution time. Return type = input type so callers retain tag information (Int2, Numeric[P, S], …).

  /** `abs(x)` — absolute value. Same type as input. */
  def abs[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("abs", e)

  /** `ceil(x)` — nearest integer greater than or equal to `x`. */
  def ceil[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("ceil", e)

  /** `floor(x)` — nearest integer less than or equal to `x`. */
  def floor[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("floor", e)

  /** `trunc(x)` — truncate toward zero. */
  def trunc[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("trunc", e)

  /** `round(x)` — round to nearest integer. */
  def round[T](e: TypedExpr[T]): TypedExpr[T] = sameTypeFn("round", e)

  /** `round(x, digits)` — round to `digits` places. Only defined for `numeric` in Postgres. */
  def round[T](e: TypedExpr[T], digits: Int): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("round(") |+| e.render |+| TypedExpr.raw(s", $digits)"),
      e.codec
    )

  /** `mod(a, b)` — remainder. Same type as input (both arguments must agree). */
  def mod[T](a: TypedExpr[T], b: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("mod(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      a.codec
    )

  /** `greatest(a, b, …)` — largest of the arguments, SQL-NULL-tolerant (Postgres: non-null wins). */
  def greatest[T](args: TypedExpr[T]*): TypedExpr[T] = {
    require(args.nonEmpty, "greatest() needs at least one argument")
    TypedExpr(
      TypedExpr.raw("greatest(") |+| TypedExpr.joined(args.map(_.render).toList, ", ") |+| TypedExpr.raw(")"),
      args.head.codec
    )
  }

  /** `least(a, b, …)` — smallest of the arguments. */
  def least[T](args: TypedExpr[T]*): TypedExpr[T] = {
    require(args.nonEmpty, "least() needs at least one argument")
    TypedExpr(
      TypedExpr.raw("least(") |+| TypedExpr.joined(args.map(_.render).toList, ", ") |+| TypedExpr.raw(")"),
      args.head.codec
    )
  }

  // -------- Math with fixed `Double` return (NULL-propagating) --------

  /** `sqrt(x)`. */
  def sqrt[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("sqrt", e)

  /** `power(a, b)` — `a` raised to the power `b`. Postgres always returns double precision. */
  def power[A, B](a: TypedExpr[A], b: TypedExpr[B])(using
    pf: PgTypeFor[Lift[A, Double]]
  ): TypedExpr[Lift[A, Double]] =
    TypedExpr(
      TypedExpr.raw("power(") |+| a.render |+| TypedExpr.raw(", ") |+| b.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  /** `exp(x)`. */
  def exp[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("exp", e)

  /** `ln(x)`. */
  def ln[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("ln", e)

  /** `log(x)` — base-10. */
  def log[T](e: TypedExpr[T])(using PgTypeFor[Lift[T, Double]]): TypedExpr[Lift[T, Double]] = doubleFn("log", e)

  /** `pi()` — constant. */
  val pi: TypedExpr[Double] = TypedExpr(TypedExpr.raw("pi()"), skunk.codec.all.float8)

  /** `random()` — uniformly distributed in `[0, 1)`. */
  val random: TypedExpr[Double] = TypedExpr(TypedExpr.raw("random()"), skunk.codec.all.float8)

  // -------- NULL handling --------

  /**
   * `nullif(a, b)` — returns NULL if `a = b`, else `a`. Result is always nullable because the caller hasn't proven
   * `a ≠ b` statically. `b` must have the same underlying type as `a` (modulo `Option` wrapping).
   */
  def nullif[T](a: TypedExpr[T], b: skunk.sharp.where.Stripped[T])(using
    pf: PgTypeFor[skunk.sharp.where.Stripped[T]]
  ): TypedExpr[Option[skunk.sharp.where.Stripped[T]]] =
    TypedExpr(
      TypedExpr.raw("nullif(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(b).render |+| TypedExpr.raw(")"),
      pf.codec.opt
    )

  // -------- String (preserve tag — return input type) --------

  /** `trim(s)` — strip leading / trailing whitespace. */
  def trim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("trim", e)

  /** `trim(chars FROM s)` — strip any of `chars` from both ends. */
  def trim[T](e: TypedExpr[T], chars: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("trim(") |+| TypedExpr.lit(chars).render |+| TypedExpr.raw(" FROM ") |+| e.render |+|
        TypedExpr.raw(")"),
      e.codec
    )

  /** `ltrim(s)`. */
  def ltrim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("ltrim", e)

  /** `rtrim(s)`. */
  def rtrim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("rtrim", e)

  /** `replace(s, from, to)`. */
  def replace[T](e: TypedExpr[T], from: String, to: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("replace(") |+| e.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(from).render |+|
        TypedExpr.raw(", ") |+| TypedExpr.lit(to).render |+| TypedExpr.raw(")"),
      e.codec
    )

  /** `substring(s FROM n)` — 1-indexed start, no length cap. */
  def substring[T](e: TypedExpr[T], from: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("substring(") |+| e.render |+| TypedExpr.raw(s" FROM $from)"), e.codec)

  /** `substring(s FROM n FOR m)` — 1-indexed start, `m` characters. */
  def substring[T](e: TypedExpr[T], from: Int, forLen: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("substring(") |+| e.render |+| TypedExpr.raw(s" FROM $from FOR $forLen)"), e.codec)

  /** `left(s, n)` — first `n` chars (negative `n` drops the last `|n|`). */
  def left[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("left(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `right(s, n)` — last `n` chars. */
  def right[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("right(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `repeat(s, n)` — `s` repeated `n` times. */
  def repeat[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("repeat(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `reverse(s)`. */
  def reverse[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("reverse", e)

  /** `regexp_replace(s, pattern, replacement)`. */
  def regexpReplace[T](e: TypedExpr[T], pattern: String, replacement: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("regexp_replace(") |+| e.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.lit(pattern).render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(replacement).render |+|
        TypedExpr.raw(")"),
      e.codec
    )

  /** `split_part(s, delim, field)` — split `s` on `delim`, return the 1-indexed `field`-th piece. */
  def splitPart[T](e: TypedExpr[T], delim: String, field: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("split_part(") |+| e.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.lit(delim).render |+| TypedExpr.raw(s", $field)"),
      e.codec
    )

  // -------- String functions with fixed `Int` return (NULL-propagating via [[Lift]]) --------

  /** `char_length(s)` — character count (synonym of [[length]]). */
  def charLength[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("char_length", e)

  /** `octet_length(s)` — byte count. */
  def octetLength[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("octet_length", e)

  /** `position(substr IN str)` — 1-indexed; returns 0 if not found, NULL if `str` is NULL. */
  def position[T](substr: String, in: TypedExpr[T])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int]] =
    TypedExpr(
      TypedExpr.raw("position(") |+| TypedExpr.lit(substr).render |+| TypedExpr.raw(" IN ") |+|
        in.render |+| TypedExpr.raw(")"),
      pf.codec
    )

  // -------- Private shape helpers — collapse repeated "fn(e)" wrappers --------

  private def sameTypeFn[T](name: String, e: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

  private def stringPreserveFn[T](name: String, e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

  private def doubleFn[T](name: String, e: TypedExpr[T])(using
    pf: PgTypeFor[Lift[T, Double]]
  ): TypedExpr[Lift[T, Double]] =
    TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), pf.codec)

  private def stringToIntFn[T](name: String, e: TypedExpr[T])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int]] =
    TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), pf.codec)

  // -------- Aggregate functions --------
  //
  // Placement: aggregates produce `TypedExpr[T]`, so they slot into any SELECT projection, HAVING predicate, or
  // ORDER BY expression. Correctness is NOT enforced at compile time — Postgres will reject a SELECT that mixes bare
  // columns with aggregates unless the bare columns appear in GROUP BY. Use `.groupBy(...)` on the builder for that.
  //
  // Return types mirror the pragmatic mapping most DSLs take:
  //   - `count*` → Long (bigint)
  //   - `sum` / `avg` → BigDecimal (numeric — superset of all Postgres numeric sum/avg outputs)
  //   - `min` / `max` → same type as input (codec carried through)

  /** `count(*)` — count of all rows, including nulls. */
  val countAll: TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(*)"), skunk.codec.all.int8)

  /** `count(expr)` — count of non-null values. */
  def count[T](expr: TypedExpr[T]): TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.int8)

  /** `count(DISTINCT expr)` — count of distinct non-null values. */
  def countDistinct[T](expr: TypedExpr[T]): TypedExpr[Long] =
    TypedExpr(TypedExpr.raw("count(DISTINCT ") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.int8)

  /**
   * `sum(expr)` — result type follows Postgres's actual rules, encoded as a type-level function via [[SumOf]]:
   *   - `sum(smallint | integer)` → `bigint` (`Long`)
   *   - `sum(bigint | numeric)` → `numeric` (`BigDecimal`)
   *   - `sum(real)` → `real` (`Float`)
   *   - `sum(double precision)` → `double precision` (`Double`)
   *
   * Codec for the result type is resolved via [[skunk.sharp.pg.PgTypeFor]] — no per-function typeclass needed. Users
   * with custom numeric opaque types can either (a) provide a `PgTypeFor` and a `SumOf` case that routes through it, or
   * (b) just use [[TypedExpr.cast]] at the call site.
   */
  def sum[I](expr: TypedExpr[I])(using pf: PgTypeFor[SumOf[I]]): TypedExpr[SumOf[I]] =
    TypedExpr(TypedExpr.raw("sum(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /**
   * `avg(expr)` — result type per Postgres: integer / bigint / numeric → `numeric` (`BigDecimal`); `real` → `double
   * precision` (`Double`); `double` → `double precision` (`Double`).
   */
  def avg[I](expr: TypedExpr[I])(using pf: PgTypeFor[AvgOf[I]]): TypedExpr[AvgOf[I]] =
    TypedExpr(TypedExpr.raw("avg(") |+| expr.render |+| TypedExpr.raw(")"), pf.codec)

  /** `min(expr)` — same type as input. */
  def min[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("min(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /** `max(expr)` — same type as input. */
  def max[T](expr: TypedExpr[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("max(") |+| expr.render |+| TypedExpr.raw(")"), expr.codec)

  /**
   * `string_agg(expr, sep)` — concatenate string values with a separator. Accepts any string-like tag on the aggregated
   * expression.
   */
  def stringAgg[T](expr: TypedExpr[T], sep: String)(using
    @annotation.unused ev: skunk.sharp.where.Stripped[T] <:< String
  ): TypedExpr[String] =
    TypedExpr(
      TypedExpr.raw("string_agg(") |+| expr.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.lit(sep).render |+| TypedExpr.raw(")"),
      skunk.codec.all.text
    )

  /** `bool_and(expr)` — true if every non-null input is true. */
  def boolAnd(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_and(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

  /** `bool_or(expr)` — true if any non-null input is true. */
  def boolOr(expr: TypedExpr[Boolean]): TypedExpr[Boolean] =
    TypedExpr(TypedExpr.raw("bool_or(") |+| expr.render |+| TypedExpr.raw(")"), skunk.codec.all.bool)

  // -------- Subquery predicates --------

  /**
   * `EXISTS (<subquery>)`. The subquery's row type doesn't matter — only existence does. Accepts any
   * [[skunk.sharp.dsl.AsSubquery]]-compatible value (compiled or un-compiled builder).
   */
  def exists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

  /** `NOT EXISTS (<subquery>)`. */
  def notExists[Q, T](sub: Q)(using ev: skunk.sharp.dsl.AsSubquery[Q, T]): TypedExpr[Boolean] = {
    val cq = ev.toCompiled(sub)
    TypedExpr(TypedExpr.raw("NOT EXISTS (") |+| cq.af |+| TypedExpr.raw(")"), skunk.codec.all.bool)
  }

}

/**
 * Preserve `T`'s nullability (or lack thereof) while changing the "base" type to `U`. Used by PG functions that have a
 * fixed non-input return type but still propagate NULL — e.g. `length(null) → null`, so
 * `length(TypedExpr[Option[String]]) → TypedExpr[Option[Int]]`.
 */
type Lift[T, U] = T match {
  case Option[x] => Option[U]
  case _         => U
}

/**
 * Type-level mapping of Postgres's `sum(I)` result type. Codec resolution for the result type flows through
 * [[skunk.sharp.pg.PgTypeFor]] — no dedicated typeclass needed. Extend by adding a case here (for well-known numeric
 * types) or by supplying your own `PgTypeFor[Out]` for a custom opaque type.
 */
type SumOf[I] = I match {
  case Short | Int       => Long
  case Long | BigDecimal => BigDecimal
  case Float             => Float
  case Double            => Double
}

/** Type-level mapping of Postgres's `avg(I)` result type. See [[SumOf]] for the same rationale. */
type AvgOf[I] = I match {
  case Short | Int | Long | BigDecimal => BigDecimal
  case Float | Double                  => Double
}
