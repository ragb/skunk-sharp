package skunk.sharp.internal

import skunk.sharp.{TypedColumn, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import scala.quoted.{Expr, Quotes, Type}

/**
 * Compile-time SQL assembly primitives. Each helper here is the macro-backed
 * equivalent of a runtime `PgOperator.infix(...)` call: instead of assembling
 * the SQL via several runtime `|+|`s over `TypedExpr.render`, the macro inlines
 * the column's singleton name at compile time and emits a single `Fragment`
 * whose `parts` list has compile-time-constant strings.
 *
 * Unsupported shapes are rejected with `report.errorAndAbort`; callers that
 * need fallback behaviour must pattern on the lambda body themselves.
 */
private[sharp] object SqlMacros {

  /**
   * Inline a binary infix comparison between a `TypedColumn[T, Null, N]` (whose name `N` is a compile-time singleton)
   * and a runtime value `rhs: T`. Produces `"<col>" <op> $<enc>` as a baked `Fragment[T]`, applied to `rhs`.
   */
  inline def columnValOp[T, Null <: Boolean, N <: String & Singleton](
    inline col: TypedColumn[T, Null, N],
    inline op:  String,
    rhs:        T
  )(using pf: PgTypeFor[T]): Where =
    ${ columnValOpImpl[T, Null, N]('col, 'op, 'rhs, 'pf) }

  private def columnValOpImpl[T: Type, Null <: Boolean: Type, N <: String & Singleton: Type](
    col: Expr[TypedColumn[T, Null, N]],
    op:  Expr[String],
    rhs: Expr[T],
    pf:  Expr[PgTypeFor[T]]
  )(using q: Quotes): Expr[Where] = {
    import q.reflect.*

    val opStr: String = op.valueOrAbort
    val colNameOpt: Option[String] = Type.valueOfConstant[N]

    colNameOpt match {
      case Some(name) =>
        // Baked SQL prefix: "<name>" <op> ($1)
        // `parts` layout for `Fragment`: Left(literal) then Right(encoder.sql) for the placeholder.
        val prefix: Expr[String] = Expr(s""""$name" $opStr """)
        '{
          val enc       = $pf.codec
          val bakedFrag = skunk.Fragment(
            List(Left($prefix), Right(enc.sql)),
            enc,
            skunk.util.Origin.unknown
          )
          TypedExpr(bakedFrag($rhs), skunk.codec.all.bool)
        }
      case None =>
        report.errorAndAbort(
          s"skunk-sharp: columnValOp requires the column's singleton name to be known at compile time " +
            s"(TypedColumn[_, _, N] with N <: String & Singleton). Found non-literal.",
          col.asTerm.pos
        )
    }
  }

}
