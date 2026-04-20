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

  def apply[T](rendered: AppliedFragment, codec0: Codec[T]): TypedExpr[T] = {
    val r = rendered
    val c = codec0
    new TypedExpr[T] {
      val render = r
      val codec  = c
    }
  }

  /** Lift a value to a bound-parameter literal `$N` expression. */
  def lit[T](value: T)(using pf: PgTypeFor[T]): TypedExpr[T] = {
    val frag = Fragment(List(Right(pf.codec.sql)), pf.codec, Origin.unknown)
    apply(frag(value), pf.codec)
  }

  /** Construct a raw, parameterless SQL fragment — used internally to emit identifiers, keywords, operator symbols. */
  def raw(sql: String): AppliedFragment = {
    val frag: Fragment[Void] = Fragment(List(Left(sql)), Void.codec, Origin.unknown)
    frag(Void)
  }

  /** Join a non-empty list of applied fragments with `sep` between them (e.g. " AND "). */
  def joined(parts: List[AppliedFragment], sep: String): AppliedFragment =
    parts match {
      case Nil          => AppliedFragment.empty
      case head :: tail => tail.foldLeft(head)((acc, p) => acc |+| raw(sep) |+| p)
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
