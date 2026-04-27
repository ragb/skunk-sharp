package skunk.sharp

import skunk.{Fragment, Void}
import skunk.sharp.pg.PgTypeFor


/**
 * Typed constructors for Postgres functions and operators. Args of inputs propagate to the result expression
 * via [[where.Where.Concat]] (Void-aware pair).
 *
 * Extension hooks third-party modules and user code lean on:
 *
 *   - `nullary` — zero-argument function (`now()`, `current_date`). Produces `TypedExpr[R, Void]`.
 *   - `unary`   — one-argument function (`lower(x)`). Returns `TypedExpr[A, X] => TypedExpr[R, X]` (Args of input
 *                 propagates).
 *   - `binary`  — two-argument function. Result Args is `Concat[X, Y]`.
 *   - `nary`    — n-argument function (`concat(a, b, c)`). Result Args is the running concat of all inputs.
 */
object PgFunction {

  /** A zero-argument function. Args = Void. */
  def nullary[R](name: String)(using pfr: PgTypeFor[R]): TypedExpr[R, Void] = {
    val frag: Fragment[Void] = TypedExpr.voidFragment(s"$name()")
    TypedExpr[R, Void](frag, pfr.codec)
  }

  /** A one-argument function: `name(arg)`. Args propagates from the argument. */
  def unary[A, R, X](name: String)(using pfr: PgTypeFor[R]): TypedExpr[A, X] => TypedExpr[R, X] =
    arg => {
      val inner = arg.fragment
      val frag  = TypedExpr.wrap(s"$name(", inner, ")")
      TypedExpr[R, X](frag, pfr.codec)
    }

  /** A two-argument function: `name(a, b)`. Args = `Concat[X, Y]`. */
  def binary[A, B, R, X, Y](name: String)(using
    pfr: PgTypeFor[R]
  ): (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, where.Where.Concat[X, Y]] =
    (a, b) => {
      val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
      val frag  = TypedExpr.wrap(s"$name(", inner, ")")
      TypedExpr[R, where.Where.Concat[X, Y]](frag, pfr.codec)
    }

  /**
   * An n-argument function: `name(a, b, c, …)`. Args is `Void` — variadic builders treat every input as
   * Void-args (Param.bind-baked or column refs); typed-Args threading through variadic functions is roadmap.
   */
  def nary[R](name: String, args: TypedExpr[?, ?]*)(using pfr: PgTypeFor[R]): TypedExpr[R, Void] = {
    if (args.isEmpty) {
      TypedExpr[R, Void](TypedExpr.voidFragment(s"$name()"), pfr.codec)
    } else {
      val inner = TypedExpr.joinedVoid(", ", args.toList.map(_.fragment))
      val frag  = TypedExpr.wrap(s"$name(", inner, ")")
      TypedExpr[R, Void](frag, pfr.codec)
    }
  }

}

/**
 * Typed constructors for Postgres infix operators. Third-party modules (jsonb `->>`, ltree `~`, …) use this to
 * expose operator extensions without touching core. Args of operands propagate via `Concat`.
 */
object PgOperator {

  /** An infix binary operator: `a op b`. Result Args = `Concat[X, Y]`. */
  def infix[A, B, R, X, Y](op: String)(using
    pfr: PgTypeFor[R]
  ): (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, where.Where.Concat[X, Y]] =
    (a, b) => {
      val frag = TypedExpr.combineSep(a.fragment, s" $op ", b.fragment)
      TypedExpr[R, where.Where.Concat[X, Y]](frag, pfr.codec)
    }

  /** A prefix unary operator: `op a`. Args propagates from the operand. */
  def prefix[A, R, X](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A, X] => TypedExpr[R, X] =
    a => {
      val frag = TypedExpr.wrap(op, a.fragment, "")
      TypedExpr[R, X](frag, pfr.codec)
    }

  /** A postfix unary operator: `a op`. Args propagates from the operand. */
  def postfix[A, R, X](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A, X] => TypedExpr[R, X] =
    a => {
      val frag = TypedExpr.wrap("", a.fragment, s" $op")
      TypedExpr[R, X](frag, pfr.codec)
    }

}
