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
 * The [[PgOperator]] sibling covers infix operators (`a op b`) — same shape, SQL syntax differs.
 *
 * The built-in [[Pg]] namespace bundles curated instances via mixin traits in [[skunk.sharp.pg.functions]] — start
 * there when looking for existing functions.
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
