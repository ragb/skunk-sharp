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
   * `Bpchar[N]` / `Text`, or their `Option` variants — anything where `Stripped[T] <:< String`). Returns the input
   * type back: tag information is preserved in the Scala type (PG returns `text` on the wire, but `Varchar[N]` /
   * `Text` etc. all decode the same text bytes through the same codec, so the value is correct).
   */
  def lower[T](e: TypedExpr[T])(using @annotation.unused ev: skunk.sharp.where.Stripped[T] <:< String): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("lower(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

  /** `upper(s)` — fold to uppercase. Shape mirrors [[lower]]. */
  def upper[T](e: TypedExpr[T])(using @annotation.unused ev: skunk.sharp.where.Stripped[T] <:< String): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("upper(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

  /**
   * `length(s)` — character count. Output nullability tracks the input: `TypedExpr[String]` → `TypedExpr[Int]`;
   * `TypedExpr[Option[Varchar[N]]]` → `TypedExpr[Option[Int]]` (PG's strict null propagation).
   */
  def length[T](e: TypedExpr[T])(using
    @annotation.unused ev: skunk.sharp.where.Stripped[T] <:< String,
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int]] =
    TypedExpr(TypedExpr.raw("length(") |+| e.render |+| TypedExpr.raw(")"), pf.codec)

  /** `concat(a, b, c, …)` — string concatenation. Each arg may carry its own string-like tag. */
  def concat(args: TypedExpr[String]*): TypedExpr[String] =
    PgFunction.nary[String]("concat", args*)

  /** Coalesce: `coalesce(a, b, c, …)` — returns the first non-null argument. */
  def coalesce[T](args: TypedExpr[T]*)(using pfr: PgTypeFor[T]): TypedExpr[T] =
    PgFunction.nary[T]("coalesce", args*)

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
   * with custom numeric opaque types can either (a) provide a `PgTypeFor` and a `SumOf` case that routes through it,
   * or (b) just use [[TypedExpr.cast]] at the call site.
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
   * `string_agg(expr, sep)` — concatenate string values with a separator. Accepts any string-like tag on the
   * aggregated expression.
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
 * Preserve `T`'s nullability (or lack thereof) while changing the "base" type to `U`. Used by PG functions that have
 * a fixed non-input return type but still propagate NULL — e.g. `length(null) → null`, so
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
  case Short | Int            => Long
  case Long | BigDecimal      => BigDecimal
  case Float                  => Float
  case Double                 => Double
}

/** Type-level mapping of Postgres's `avg(I)` result type. See [[SumOf]] for the same rationale. */
type AvgOf[I] = I match {
  case Short | Int | Long | BigDecimal => BigDecimal
  case Float | Double                  => Double
}
