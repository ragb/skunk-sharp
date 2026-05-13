package skunk.sharp.contrib.hstore

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * `hstore` operator surface. Method names are spelt out (`.get`, `.hasKey`, `.contains`, …) rather than tracking
 * Postgres's symbolic operators (`->`, `?`, `@>`, …) — same convention as the rest of the contribs.
 */
extension [A](lhs: TypedExpr[Hstore, A]) {

  /** `lhs -> key` — value for `key`, NULL if absent. Returns `Option[String]`. */
  inline def get[B](key: TypedExpr[String, B]): TypedExpr[Option[String], Where.Concat[A, B]] = {
    val frag = TypedExpr.combineSepInl[A, B](lhs.fragment, " -> ", key.fragment)
    TypedExpr[Option[String], Where.Concat[A, B]](frag, skunk.codec.all.text.opt)
  }

  /** `lhs ? key` — does `key` exist? */
  inline def hasKey[B](key: TypedExpr[String, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Hstore, String, Boolean, A, B]("?")(lhs, key)
    Where(expr.fragment)
  }

  /** `lhs @> rhs` — does `lhs` contain all key/value pairs of `rhs`? */
  inline def contains[B](rhs: TypedExpr[Hstore, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Hstore, Hstore, Boolean, A, B]("@>")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs <@ rhs` — is `lhs` contained in `rhs`? */
  inline def containedBy[B](rhs: TypedExpr[Hstore, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Hstore, Hstore, Boolean, A, B]("<@")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs - key` — drop a single key, returning the resulting hstore. */
  inline def deleteKey[B](key: TypedExpr[String, B]): TypedExpr[Hstore, Where.Concat[A, B]] =
    PgOperator.infix[Hstore, String, Hstore, A, B]("-")(lhs, key)

}
