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

}
