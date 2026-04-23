package skunk.sharp

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.pg.PgTypeFor
import skunk.util.Origin

/**
 * A typed SQL expression — the universal vocabulary of the DSL.
 *
 * Everything operators, functions, and WHERE clauses produce and consume is a `TypedExpr[T]`. The primitive leaves are
 * [[TypedColumn]] (a column reference) and `TypedExpr.lit` (a bound-parameter literal). Third-party modules add new
 * operators and functions by returning `TypedExpr[...]`; no changes to core are required.
 */
trait TypedExpr[T] {
  def render: AppliedFragment
  def codec: Codec[T]
}

object TypedExpr {

  /**
   * Construct a `TypedExpr` whose rendered SQL fragment is computed **lazily** — by-name argument plus `lazy val`
   * inside. Operators, functions, and subquery lifters (`.asExpr`, `Pg.exists`, `Pg.notExists`, …) that compose other
   * `TypedExpr`s by touching their `render` field chain their work off this one. That means nothing is materialised
   * until the outermost `SelectBuilder.compile` / `ProjectedSelect.compile` walks its tree of expressions and pulls on
   * `.render` — the single terminal "render SQL now" point.
   *
   * The laziness matters most for correlated subquery uses (`Pg.exists(inner)`) where `inner` is a `SelectBuilder`
   * whose own `.compile` only works once the outer view has been fixed. Eager rendering at `Pg.exists(...)`-call time
   * would force the inner compile inside the outer's `.where` lambda — too early. The `lazy val` defers it.
   */
  def apply[T](rendered: => AppliedFragment, codec0: Codec[T]): TypedExpr[T] = {
    val c = codec0
    new TypedExpr[T] {
      lazy val render: AppliedFragment = rendered
      val codec: Codec[T]              = c
    }
  }

  /**
   * Lift a **compile-time primitive literal** into a `TypedExpr[T]`. Supported literal types: `Boolean`, `Int`, `Long`,
   * `Short`, `Byte`, `Float`, `Double`. The literal is rendered inline in the SQL text (`TRUE` / `42` /
   * `'Infinity'::float8` / …), never as a bound `$N` parameter.
   *
   * Anything else — runtime variables, strings, UUIDs, timestamps, arrays, refined / tag types, user-defined — is a
   * **compile error**. The error message points at:
   *
   *   - WHERE operators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `.in(xs)`, `.like(p)`, `.contains(...)`, …) which
   *     parameterise their RHS value directly. `col === x` for a runtime `x` renders as `col = $N`.
   *   - [[skunk.sharp.dsl.param]]`(v)`, the explicit runtime-value escape hatch. Use it only when you need a
   *     `TypedExpr[T]` from a runtime value in a position no operator handles (e.g. `Pg.ceil(param(bigDec))`).
   *
   * `lit` is named for the SQL output shape: an inline literal. If you reach for it with a runtime value you're asking
   * for plan-cache misses and (for strings) injection risk — the macro rejects that up front.
   */
  inline def lit[T](inline value: T)(using pf: PgTypeFor[T]): TypedExpr[T] =
    ${ litMacro.impl[T]('value, 'pf) }

  /**
   * Runtime-parameterised literal — the fallback path. Exposed so the `lit` macro can splice a call when the value is
   * not a compile-time constant. Not for general use; prefer [[lit]] which decides inline vs parameterised for you.
   */
  def parameterised[T](value: T)(using pf: PgTypeFor[T]): TypedExpr[T] = {
    val frag = Fragment(List(Right(pf.codec.sql)), pf.codec, Origin.unknown)
    apply(frag(value), pf.codec)
  }

  /**
   * Render a `Float` literal as its SQL form. Always emits a `::float4` cast — Postgres's default numeric-literal parse
   * type is `numeric`, and functions like `sqrt(9.0)` then return `numeric` instead of the expected `double precision`,
   * which blows up the skunk decoder. The explicit cast pins the type.
   *
   * IEEE specials (`NaN`, `±Infinity`) can't be written as bare numeric tokens in SQL — Postgres accepts them only as
   * quoted strings cast to the target float type. Public so the `lit` macro can call it from spliced code.
   */
  def renderFloat(v: Float): String = v match {
    case x if java.lang.Float.isNaN(x) => "'NaN'::float4"
    case Float.PositiveInfinity        => "'Infinity'::float4"
    case Float.NegativeInfinity        => "'-Infinity'::float4"
    case x                             => s"${x.toString}::float4"
  }

  /** Same as [[renderFloat]] but for `Double` — emits a `::float8` cast for identical reasons. */
  def renderDouble(v: Double): String = v match {
    case x if java.lang.Double.isNaN(x) => "'NaN'::float8"
    case Double.PositiveInfinity        => "'Infinity'::float8"
    case Double.NegativeInfinity        => "'-Infinity'::float8"
    case x                              => s"${x.toString}::float8"
  }

  /**
   * Construct a raw, parameterless SQL fragment — used internally to emit identifiers, keywords, operator symbols.
   *
   * Fast path for structural tokens that appear in every compiled query (`)`, `(`, `, `, ` AND `, ` WHERE `, ` FROM `,
   * ` ON `, ` GROUP BY `, ` HAVING `, ` ORDER BY `): return a process-wide-cached `AppliedFragment` instead of
   * allocating a fresh `Fragment` + `AppliedFragment` per call. A single `.compile` on a moderately complex SELECT
   * traverses these dozens of times; without the cache every invocation is a fresh allocation and a list-of-lists
   * merge when `|+|`-ed.
   */
  def raw(sql: String): AppliedFragment =
    sql match {
      case ")"                        => skunk.sharp.internal.RawConstants.CLOSE_PAREN
      case "("                        => skunk.sharp.internal.RawConstants.OPEN_PAREN
      case ", "                       => skunk.sharp.internal.RawConstants.COMMA_SEP
      case " AND "                    => skunk.sharp.internal.RawConstants.AND
      case " WHERE "                  => skunk.sharp.internal.RawConstants.WHERE
      case " FROM "                   => skunk.sharp.internal.RawConstants.FROM
      case " ON "                     => skunk.sharp.internal.RawConstants.ON
      case " GROUP BY "               => skunk.sharp.internal.RawConstants.GROUP_BY
      case " HAVING "                 => skunk.sharp.internal.RawConstants.HAVING
      case " ORDER BY "               => skunk.sharp.internal.RawConstants.ORDER_BY
      case "SELECT "                  => skunk.sharp.internal.RawConstants.SELECT
      case "VALUES "                  => skunk.sharp.internal.RawConstants.VALUES
      case " RETURNING "              => skunk.sharp.internal.RawConstants.RETURNING
      case " USING "                  => skunk.sharp.internal.RawConstants.USING
      case " ON CONFLICT DO NOTHING"  => skunk.sharp.internal.RawConstants.ON_CONFLICT_DO_NOTHING
      case _ =>
        val frag: Fragment[Void] = Fragment(List(Left(sql)), Void.codec, Origin.unknown)
        frag(Void)
    }

  /** Join a non-empty list of applied fragments with `sep` between them (e.g. " AND "). */
  def joined(parts: List[AppliedFragment], sep: String): AppliedFragment =
    parts match {
      case Nil          => AppliedFragment.empty
      case head :: tail =>
        // Allocate the separator once, reuse it in every |+|.
        val sepAf = raw(sep)
        tail.foldLeft(head)((acc, p) => acc |+| sepAf |+| p)
    }

}

/**
 * Postgres-side cast: `expr::<type>`. Turns a `TypedExpr[T]` into a `TypedExpr[U]`. Useful in projections where the
 * underlying column stores a different Postgres type than the Scala value you want to decode into, and in WHERE
 * comparisons that need an explicit type coercion.
 *
 * {{{
 *   users.select(u => u.age.cast[Long])                        // SELECT "age"::int8 FROM "users"
 *   users.select.where(u => u.id.cast[String] === "abc-123")   // SELECT … WHERE "id"::text = $1
 * }}}
 */
extension [T](expr: TypedExpr[T]) {

  def cast[U](using pf: PgTypeFor[U]): TypedExpr[U] = {
    val castName = skunk.sharp.pg.PgTypes.castName(skunk.sharp.pg.PgTypes.typeOf(pf.codec))
    TypedExpr(expr.render |+| TypedExpr.raw(s"::$castName"), pf.codec)
  }

  /**
   * Render the expression with a SQL column alias: `<expr> AS "<name>"`. The alias is captured in the result type via
   * [[AliasedExpr]], so downstream match types can inspect the name (e.g. a future compile-time GROUP BY check, or
   * alias-in-clause resolution, can walk the projection tuple and collect alias singletons).
   *
   * Meaningful in SELECT projections — Postgres accepts column aliases there and in `ORDER BY` / `GROUP BY` / `HAVING`
   * (but not in `WHERE`, per SQL's logical-evaluation order). We don't gate the placement; Postgres raises a clear
   * error for misuse.
   */
  def as[N <: String & Singleton](name: N): AliasedExpr[T, N] = {
    val rendered = expr.render |+| TypedExpr.raw(s""" AS "$name"""")
    val c        = expr.codec
    new AliasedExpr[T, N] {
      val aliasName = name
      val render    = rendered
      val codec     = c
    }
  }

}

/**
 * A [[TypedExpr]] carrying a compile-time-known alias name (`<expr> AS "<N>"`). Produced by `expr.as("name")`; distinct
 * from plain `TypedExpr` so projections that use `.as` can be introspected at the type level by match types that care
 * about the alias (e.g. future compile-time checks for GROUP BY coverage or alias-in-clause resolution).
 */
trait AliasedExpr[T, N <: String & Singleton] extends TypedExpr[T] {
  def aliasName: N
}
