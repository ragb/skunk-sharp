package skunk.sharp

import skunk.{Codec, Fragment, Void}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/**
 * Typed constructors for Postgres functions and operators. Args of inputs propagate to the result expression via
 * [[where.Where.Concat]] (Void-aware pair).
 *
 * Extension hooks third-party modules and user code lean on:
 *
 *   - `nullary` — zero-argument function (`now()`, `current_date`). Produces `TypedExpr[R, Void]`.
 *   - `unary` — one-argument function (`lower(x)`). Returns `TypedExpr[A, X] => TypedExpr[R, X]` (Args of input
 *     propagates).
 *   - `binary` — two-argument function. Result Args is `Concat[X, Y]`.
 *   - `naryTypedFold` — N-argument helper used by variadic builders (`coalesce`, `greatest`, `least`, `concat`) to
 *     thread each input's typed Args via [[Where.FoldConcat]] (inline-projected).
 */
object PgFunction {

  /**
   * Typed N-ary helper: render `name(item, item, …)` with each item's typed `Args` threaded into a single `Args` slot
   * via [[Where.FoldConcat]]. Because `Where.Concat` is smart-flat, the resulting `Args` is the non-Void slots
   * flattened into a single tuple (e.g. `coalesce(Param[String], col, Param[String])` → `(String, String)`). Used by
   * variadic builders (`coalesce` / `greatest` / `least` / `concat`) at every arity. Inline so the per-slot
   * `projectFoldConcat` dispatch reduces with the concrete `Tup` shape at the caller's site.
   */
  private[sharp] inline def naryTypedFold[T, Tup <: NonEmptyTuple](
    name: String,
    items: List[Fragment[?]],
    codec: Codec[T]
  ): TypedExpr[T, Where.FoldConcat[Tup]] = {
    val combined = TypedExpr.combineList[Where.FoldConcat[Tup]](
      items,
      ", ",
      Where.projFold[Tup]
    )
    val frag = TypedExpr.wrap(s"$name(", combined, ")")
    TypedExpr(frag, codec)
  }

  /** `name(a)` with an explicit result codec. Args propagate from the argument. */
  def call1[R, A](name: String, a: TypedExpr[?, A], codec: Codec[R]): TypedExpr[R, A] =
    callF1[R, A](name, a.fragment, codec)

  /** `name(a, b, c)` with an explicit result codec. Args = the flat concat of the three. */
  inline def call3[R, X, Y, Z](
    name: String,
    a: TypedExpr[?, X],
    b: TypedExpr[?, Y],
    c: TypedExpr[?, Z],
    codec: Codec[R]
  )
    : TypedExpr[R, where.Where.Concat[where.Where.Concat[X, Y], Z]] =
    callF3[R, X, Y, Z](name, a.fragment, b.fragment, c.fragment, codec)

  /** Fragment-level [[call1]] — for arguments that aren't a plain `TypedExpr` (e.g. a `::regconfig`-cast literal). */
  def callF1[R, A](name: String, a: Fragment[A], codec: Codec[R]): TypedExpr[R, A] =
    TypedExpr[R, A](TypedExpr.wrap(name + "(", a, ")"), codec)

  /** Fragment-level two-argument call. */
  inline def callF2[R, X, Y](name: String, a: Fragment[X], b: Fragment[Y], codec: Codec[R])
    : TypedExpr[R, where.Where.Concat[X, Y]] =
    TypedExpr[R, where.Where.Concat[X, Y]](
      TypedExpr.wrap(name + "(", TypedExpr.combineSepInl[X, Y](a, ", ", b), ")"),
      codec
    )

  /** Fragment-level three-argument call. */
  inline def callF3[R, X, Y, Z](name: String, a: Fragment[X], b: Fragment[Y], c: Fragment[Z], codec: Codec[R])
    : TypedExpr[R, where.Where.Concat[where.Where.Concat[X, Y], Z]] = {
    val ab = TypedExpr.combineSepInl[X, Y](a, ", ", b)
    TypedExpr[R, where.Where.Concat[where.Where.Concat[X, Y], Z]](
      TypedExpr.wrap(name + "(", TypedExpr.combineSepInl[where.Where.Concat[X, Y], Z](ab, ", ", c), ")"),
      codec
    )
  }

  /**
   * `name(a, b)` with an explicit result codec. Args = `Concat[X, Y]`, split back per side by `projectConcat` at the
   * (inline) call site — correct whatever shape each side's Args has (Void, scalar, or a multi-Param tuple).
   */
  inline def call2[R, X, Y](inline name: String, a: TypedExpr[?, X], b: TypedExpr[?, Y], codec: Codec[R])
    : TypedExpr[R, where.Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", b.fragment)
    TypedExpr[R, where.Where.Concat[X, Y]](TypedExpr.wrap(name + "(", inner, ")"), codec)
  }

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
  inline def binary[A, B, R, X, Y](name: String)(using
    pfr: PgTypeFor[R]
  ): (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, where.Where.Concat[X, Y]] =
    (a, b) => {
      val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", b.fragment)
      val frag  = TypedExpr.wrap(s"$name(", inner, ")")
      TypedExpr[R, where.Where.Concat[X, Y]](frag, pfr.codec)
    }

}

/**
 * Typed constructors for Postgres infix operators. Third-party modules (jsonb `->>`, ltree `~`, …) use this to expose
 * operator extensions without touching core. Args of operands propagate via `Concat`.
 */
object PgOperator {

  /**
   * `(l op r)` — the rendering every infix operator uses. Always parenthesised: Postgres's many "other operator"s share
   * one precedence level and associate left, so an unparenthesised operand would silently regroup (`v <=> q * 2` is
   * `v <=> (q * 2)`).
   */
  inline def binary[X, Y](op: String, l: Fragment[X], r: Fragment[Y]): Fragment[where.Where.Concat[X, Y]] =
    TypedExpr.wrap("(", TypedExpr.combineSepInl[X, Y](l, " " + op + " ", r), ")")

  /** An infix binary operator: `(a op b)`. Result Args = `Concat[X, Y]`. */
  inline def infix[A, B, R, X, Y](op: String)(using
    pfr: PgTypeFor[R]
  ): (TypedExpr[A, X], TypedExpr[B, Y]) => TypedExpr[R, where.Where.Concat[X, Y]] =
    (a, b) => TypedExpr[R, where.Where.Concat[X, Y]](binary[X, Y](op, a.fragment, b.fragment), pfr.codec)

  /** A prefix unary operator: `(op a)`. Args propagates from the operand. */
  def prefix[A, R, X](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A, X] => TypedExpr[R, X] =
    a => TypedExpr[R, X](TypedExpr.wrap("(" + op, a.fragment, ")"), pfr.codec)

  /** A postfix unary operator: `(a op)`. Args propagates from the operand. */
  def postfix[A, R, X](op: String)(using pfr: PgTypeFor[R]): TypedExpr[A, X] => TypedExpr[R, X] =
    a => TypedExpr[R, X](TypedExpr.wrap("(", a.fragment, " " + op + ")"), pfr.codec)

}
