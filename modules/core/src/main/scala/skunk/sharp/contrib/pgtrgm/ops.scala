package skunk.sharp.contrib.pgtrgm

import scala.annotation.unused
import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.ops.Stripped
import skunk.sharp.where.Where

/**
 * Trigram operators on `String`-shaped expressions. Method names use plain English (`.similarTrgm`, `.trgmDistance`)
 * instead of Postgres's symbolic operators (`%`, `<->`, …) so they don't fight for namespace with anything else and stay
 * obviously trigram-specific at the call site.
 *
 * The `Stripped[T] <:< String` evidence lets the operators apply to any tag whose underlying type is `String`
 * (`Citext`, `Varchar[N]`, `LTree`, …) without ceremony.
 */
extension [T, A](lhs: TypedExpr[T, A]) {

  /** `lhs % rhs` — trigram similarity above the current threshold. */
  inline def similarTrgm[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[T, String, Boolean, A, B]("%")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs <-> rhs` — trigram distance. Use in `ORDER BY` for "closest match first". */
  inline def trgmDistance[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgOperator.infix[T, String, Float, A, B]("<->")(lhs, rhs)

  /** `lhs <% rhs` — word-similarity above threshold. */
  inline def wordSimilar[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[T, String, Boolean, A, B]("<%")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs <<% rhs` — strict-word-similarity above threshold. */
  inline def strictWordSimilar[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[T, String, Boolean, A, B]("<<%")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs <<-> rhs` — word-similarity distance. */
  inline def wordTrgmDistance[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgOperator.infix[T, String, Float, A, B]("<<->")(lhs, rhs)

  /** `lhs <->> rhs` — strict-word-similarity distance. */
  inline def strictWordTrgmDistance[B](rhs: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgOperator.infix[T, String, Float, A, B]("<->>")(lhs, rhs)

}
