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

  /**
   * Inline a 2-argument infix predicate: `<lhs> <sep1> <rhs1> <sep2> <rhs2>` — `BETWEEN lo AND hi`,
   * `NOT BETWEEN lo AND hi`, `BETWEEN SYMMETRIC lo AND hi`. When the LHS is a `TypedColumn`, the prefix
   * `"<col> <sep1> "` is baked into a single `Fragment[Void]`; the two RHS values are still bound via
   * `TypedExpr.parameterised` and composed with `|+|` — saves the `lhs.render` allocation and one
   * `raw(...)` allocation over the hand-rolled chain.
   */
  inline def infix2[T, S](
    inline sep1: String,
    inline sep2: String,
    inline lhs:  TypedExpr[T],
    rhs1:        S,
    rhs2:        S
  )(using pf: PgTypeFor[S]): Where =
    ${ infix2Impl[T, S]('sep1, 'sep2, 'lhs, 'rhs1, 'rhs2, 'pf) }

  private def infix2Impl[T: Type, S: Type](
    sep1: Expr[String],
    sep2: Expr[String],
    lhs:  Expr[TypedExpr[T]],
    rhs1: Expr[S],
    rhs2: Expr[S],
    pf:   Expr[PgTypeFor[S]]
  )(using q: Quotes): Expr[Where] = {
    import q.reflect.*

    def runtime: Expr[Where] =
      '{
        TypedExpr(
          $lhs.render |+|
            TypedExpr.raw(" " + $sep1 + " ") |+|
            TypedExpr.parameterised[S]($rhs1)(using $pf).render |+|
            TypedExpr.raw(" " + $sep2 + " ") |+|
            TypedExpr.parameterised[S]($rhs2)(using $pf).render,
          skunk.codec.all.bool
        )
      }

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          val prefix = skunk.AppliedFragment(
            skunk.Fragment(
              List(Left(col.sqlRef + " " + $sep1 + " ")),
              skunk.Void.codec,
              skunk.util.Origin.unknown
            ),
            skunk.Void
          )
          TypedExpr(
            prefix |+|
              TypedExpr.parameterised[S]($rhs1)(using $pf).render |+|
              TypedExpr.raw(" " + $sep2 + " ") |+|
              TypedExpr.parameterised[S]($rhs2)(using $pf).render,
            skunk.codec.all.bool
          )
        }
      case _ => runtime
    }
  }

  /**
   * Build a parameterless `AppliedFragment` rendering the LHS followed by a literal op string. Used by operators whose
   * RHS is already assembled as an `AppliedFragment` at call time (e.g. `IN (…)` with a variadic value list or a
   * subquery). When the LHS is a `TypedColumn`, the whole `"<col> <op> "` literal is a single baked
   * `Fragment[Void]`; otherwise falls back to `lhs.render |+| raw(" <op> ")`.
   */
  inline def prefix[T](inline lhs: TypedExpr[T], inline op: String): skunk.AppliedFragment =
    ${ prefixImpl[T]('lhs, 'op) }

  private def prefixImpl[T: Type](
    lhs: Expr[TypedExpr[T]],
    op:  Expr[String]
  )(using q: Quotes): Expr[skunk.AppliedFragment] = {
    import q.reflect.*

    def runtime: Expr[skunk.AppliedFragment] =
      '{ $lhs.render |+| TypedExpr.raw(" " + $op + " ") }

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          skunk.AppliedFragment(
            skunk.Fragment(
              List(Left(col.sqlRef + " " + $op + " ")),
              skunk.Void.codec,
              skunk.util.Origin.unknown
            ),
            skunk.Void
          )
        }
      case _ => runtime
    }
  }

  /**
   * Inline a postfix no-argument predicate `<lhs> <suffix>` — `IS NULL`, `IS NOT NULL`, `IS TRUE`, …. When the LHS is a
   * `TypedColumn`, the macro emits a single `Fragment[Void]` whose `parts` is `List(Left(col.sqlRef + suffix))` —
   * no encoder, no `$N`, one allocation. Runtime fallback for other LHS shapes.
   */
  inline def postfix[T](inline lhs: TypedExpr[T], inline suffix: String): Where =
    ${ postfixImpl[T]('lhs, 'suffix) }

  private def postfixImpl[T: Type](
    lhs:    Expr[TypedExpr[T]],
    suffix: Expr[String]
  )(using q: Quotes): Expr[Where] = {
    import q.reflect.*

    def runtime: Expr[Where] =
      '{
        TypedExpr(
          $lhs.render |+| TypedExpr.raw($suffix),
          skunk.codec.all.bool
        )
      }

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          TypedExpr(
            skunk.AppliedFragment(
              skunk.Fragment(
                List(Left(col.sqlRef + $suffix)),
                skunk.Void.codec,
                skunk.util.Origin.unknown
              ),
              skunk.Void
            ),
            skunk.codec.all.bool
          )
        }
      case _ => runtime
    }
  }

}
