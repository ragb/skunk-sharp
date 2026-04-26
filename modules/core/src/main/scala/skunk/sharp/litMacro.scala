package skunk.sharp

import skunk.Void
import skunk.sharp.pg.PgTypeFor

import scala.quoted.{Expr, Quotes, Type}

/**
 * Compile-time machinery behind [[TypedExpr.lit]]. **Literal-only**: the argument must be a compile-time constant
 * (`Boolean`, `Int`, `Long`, `Short`, `Byte`, `Float`, `Double`, or `String`). Anything else is a compile error
 * pointing at:
 *
 *   - [[Param]] — for execute-time-bound runtime values (the standard static-query path).
 *   - [[Param.bind]] — the explicit "bake a runtime value into a Void-args fragment now" escape hatch.
 *
 * Returns `TypedExpr[T, Void]` — primitive literals contribute no parameters.
 */
private[sharp] object litMacro {

  def impl[T: Type](value: Expr[T], pf: Expr[PgTypeFor[T]])(using q: Quotes): Expr[TypedExpr[T, Void]] = {
    import q.reflect.*

    def rendered(sql: String): Expr[TypedExpr[T, Void]] = {
      val sqlExpr: Expr[String] = Expr(sql)
      '{
        val frag = TypedExpr.voidFragment($sqlExpr)
        TypedExpr[T, Void](frag, $pf.codec)
      }
    }

    def reject: Expr[TypedExpr[T, Void]] =
      report.errorAndAbort(
        "skunk-sharp: `lit(v)` requires `v` to be a compile-time literal " +
          "(Boolean / Int / Long / Short / Byte / Float / Double / String). " +
          "For runtime values, use `Param[T]` (defers binding to execute time) or `Param.bind(v)` (bakes the value " +
          "into a Void-args fragment now).",
        value.asTerm.pos
      )

    def sqlStringLiteral(s: String): String = s"'${s.replace("'", "''")}'"

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
      case Literal(StringConstant(v))  => rendered(sqlStringLiteral(v))
      case _                           => reject
    }
  }

}
