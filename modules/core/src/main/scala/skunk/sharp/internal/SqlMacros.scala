package skunk.sharp.internal

import skunk.sharp.{PgOperator, TypedColumn, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import scala.quoted.{Expr, Quotes, Type}

/**
 * Compile-time SQL assembly primitives. Each helper here is the macro-backed
 * equivalent of a runtime `PgOperator.infix(...)` call: when the LHS is
 * statically known to be a `TypedColumn[_, _, N]` with a singleton name `N`,
 * the macro inlines the name and emits a single `skunk.Fragment` whose `parts`
 * list has compile-time-constant strings, bypassing the runtime `|+|` chain.
 *
 * For LHS shapes we can't resolve at compile time (any `TypedExpr[T]` that
 * isn't a `TypedColumn` literal — e.g. `lower(col)`, `col.cast[_]`, a subquery
 * `.asExpr`) the macro emits the same runtime expression the hand-written
 * `valOp` helper would have produced. Behaviour is identical either way.
 */
private[sharp] object SqlMacros {

  /**
   * Inline a binary infix comparison `lhs <op> <rhs>` where the RHS is a runtime
   * value encoded as a parameter. Dispatches at compile time:
   *   - LHS is a `TypedColumn[_, _, N]` (N singleton) → baked fragment path.
   *   - anything else → runtime `PgOperator.infix` + `TypedExpr.parameterised`.
   */
  inline def infix[T, S](
    inline op:  String,
    inline lhs: TypedExpr[T],
    rhs:        S
  )(using pf: PgTypeFor[S]): Where =
    ${ infixImpl[T, S]('op, 'lhs, 'rhs, 'pf) }

  private def infixImpl[T: Type, S: Type](
    op:  Expr[String],
    lhs: Expr[TypedExpr[T]],
    rhs: Expr[S],
    pf:  Expr[PgTypeFor[S]]
  )(using q: Quotes): Expr[Where] = {
    import q.reflect.*

    val opStr: String = op.valueOrAbort

    // Runtime fallback — the existing PgOperator.infix path.
    def runtime: Expr[Where] =
      '{ PgOperator.infix[T, S, Boolean]($op)($lhs, TypedExpr.parameterised[S]($rhs)(using $pf)) }

    // When the LHS is a TypedColumn, we can bake the op symbol + encoder placeholder at compile time.
    // The column identifier itself (`"name"` or `"alias"."name"` — depending on whether JOINs / subqueries
    // qualified the column) is a runtime value via TypedColumn.sqlRef. Baking it too would break qualified
    // uses. We still win: one Fragment allocation + one String concat, vs several |+| and intermediate
    // AppliedFragments in the PgOperator.infix runtime path.
    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        val suffix: Expr[String] = Expr(s" $opStr ")
        '{
          val col       = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          val enc       = $pf.codec
          val bakedFrag = skunk.Fragment(
            List(Left(col.sqlRef + $suffix), Right(enc.sql)),
            enc,
            skunk.util.Origin.unknown
          )
          TypedExpr(bakedFrag($rhs), skunk.codec.all.bool)
        }
      case _ => runtime
    }
  }

  /**
   * Legacy POC helper — kept for the explicit-baked-path test that exercises the
   * macro without going through the `===` / `>=` / ... extension methods.
   */
  inline def columnValOp[T, Null <: Boolean, N <: String & Singleton](
    inline col: TypedColumn[T, Null, N],
    inline op:  String,
    rhs:        T
  )(using pf: PgTypeFor[T]): Where =
    infix[T, T](op, col, rhs)

}
