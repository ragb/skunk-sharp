package skunk.sharp

import skunk.sharp.pg.PgTypeFor

import scala.quoted.{Expr, Quotes, Type}

/**
 * Compile-time machinery behind [[TypedExpr.lit]]. **Literal-only**: the argument must be a compile-time primitive
 * constant (`Boolean`, `Int`, `Long`, `Short`, `Byte`, `Float`, `Double`). Anything else — runtime variables, strings,
 * UUIDs, timestamps, arrays, refined / tag types, user-defined — is a compile error pointing at the available
 * alternatives:
 *
 *   - WHERE operators (`===`, `<`, `.in(...)`, `.like(...)`, array `.contains(...)`, …) take their RHS value directly
 *     and parameterise it internally. That covers the common case.
 *   - [[skunk.sharp.dsl.param]]`(v)` is the explicit "bind this runtime value as `$N`" escape hatch for the rare cases
 *     when you need a `TypedExpr[T]` from a runtime value in a position no operator handles (e.g. an RHS passed to a
 *     non-operator extension).
 *
 * `lit` means "render as a literal in the SQL text". Falling through to a bound parameter would defeat the point — and
 * for Strings specifically would either be a SQL-injection footgun or confusing (inlining vs parameterising the same
 * value decided by type alone). Strict semantics → one error, two clean alternatives.
 *
 * Implementation: pattern-match on the argument's `Term` tree for `Literal(*Constant(v))` shapes, after walking through
 * `Inlined` / `Block(Nil, …)` / `Typed` wrappers that `inline def` forwarding can introduce. Emits a pointed
 * `report.errorAndAbort` otherwise.
 */
private[sharp] object litMacro {

  def impl[T: Type](value: Expr[T], pf: Expr[PgTypeFor[T]])(using q: Quotes): Expr[TypedExpr[T]] = {
    import q.reflect.*

    def rendered(sql: String): Expr[TypedExpr[T]] = {
      val sqlExpr: Expr[String] = Expr(sql)
      '{ TypedExpr(TypedExpr.raw($sqlExpr), $pf.codec) }
    }

    def reject: Expr[TypedExpr[T]] =
      report.errorAndAbort(
        "skunk-sharp: `lit(v)` requires `v` to be a compile-time primitive literal " +
          "(Boolean / Int / Long / Short / Byte / Float / Double). " +
          "For runtime values, the WHERE operators (`col === v`, `col < v`, `col.in(xs)`, `col.contains(xs)`, `col.like(p)`, …) " +
          "take their RHS directly and parameterise it as `$N`. " +
          "Use `skunk.sharp.dsl.param(v)` only when you explicitly need a `TypedExpr[T]` from a runtime value in a position " +
          "no operator handles.",
        value.asTerm.pos
      )

    // `inline def` forwarding wraps the argument in `Inlined(…)` nodes; the literal we want is nested inside. Walk
    // through all layers (plus blocks with empty statements, which the inline expander can introduce) to the
    // underlying term before matching the literal constant.
    def unwrap(t: Term): Term = t match {
      case Inlined(_, Nil, inner)      => unwrap(inner)
      case Inlined(_, bindings, inner) =>
        if (bindings.isEmpty) unwrap(inner) else t
      case Block(Nil, inner) => unwrap(inner)
      case Typed(inner, _)   => unwrap(inner)
      case other             => other
    }

    unwrap(value.asTerm) match {
      case Literal(BooleanConstant(v)) => rendered(if (v) "TRUE" else "FALSE")
      case Literal(IntConstant(v))     => rendered(v.toString)
      case Literal(LongConstant(v))    => rendered(v.toString)
      case Literal(ShortConstant(v))   => rendered(v.toString)
      case Literal(ByteConstant(v))    => rendered(v.toString)
      case Literal(FloatConstant(v))   => rendered(TypedExpr.renderFloat(v))
      case Literal(DoubleConstant(v))  => rendered(TypedExpr.renderDouble(v))
      case _                           => reject
    }
  }

}
