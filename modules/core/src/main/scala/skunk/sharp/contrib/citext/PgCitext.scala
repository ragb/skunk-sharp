package skunk.sharp.contrib.citext

import skunk.sharp.TypedExpr

/**
 * Helpers for the `citext` extension. Cast a regular `String` expression to `citext` so subsequent comparisons run
 * case-insensitively without touching the column declaration.
 *
 * Mix into your own `Pg`-like bundle or call via the namespace object:
 *
 * {{{
 *   import skunk.sharp.contrib.citext.*
 *
 *   users.select.where(u => PgCitext.toCitext(u.email) === lit(Citext("Alice@Example.COM"))).compile.option(s)
 * }}}
 */
trait PgCitext {

  /** Cast a `TypedExpr[String]` to `TypedExpr[Citext]` via `<expr>::citext`. */
  inline def toCitext[A](e: TypedExpr[String, A]): TypedExpr[Citext, A] = {
    val frag = TypedExpr.wrap("(", e.fragment, ")::citext")
    TypedExpr[Citext, A](frag, Citext.codec)
  }

}

object PgCitext extends PgCitext
