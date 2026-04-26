package skunk.sharp.ops

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.internal.SqlMacros
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import scala.annotation.unused

/**
 * The v0 expression-level operator set: `=, <>, <, <=, >, >=, IN, LIKE, IS NULL`. Every operator produces a
 * `Where[A]` — a typed predicate carrying its captured-parameter tuple as a visible type parameter. `Where[A]`
 * is itself a `TypedExpr[Boolean]`, so it slots wherever a boolean expression is valid in Postgres: WHERE
 * clauses, SELECT projections, HAVING, ORDER BY, function arguments, CASE WHEN predicates.
 *
 * Operators are *extension methods* on `TypedExpr[T]` so third-party modules add new ones without touching core.
 *
 * **Nullable columns.** If a column is declared nullable, comparisons like `col === value` take the underlying
 * value type, not `Option[value]`. Trying to compare against `None` is a compile error — use `.isNull` /
 * `.isNotNull` instead. See [[Stripped]].
 *
 * **Args threading.** Each operator's result `Where[A]` carries the RHS's scalar type as `A`. Combinators on
 * `Where` (`&&`, `||`, `!`) pair their args (`Where[A] && Where[B]` is `Where[(A, B)]`), surfacing the full
 * captured-parameter tuple at the builder's `.compile` site as `CompiledQuery[Args, R]`.
 */

/**
 * Type-level alias: stripped of the outermost `Option[_]` if there is one, otherwise unchanged. Drives the RHS
 * of comparison operators so callers pass the underlying value even for nullable columns.
 */
type Stripped[T] = T match {
  case Option[x] => x
  case _         => T
}

/**
 * Infix comparison with a runtime-parameterised RHS. All the value-on-the-right operators share this shape.
 *
 * `inline` + macro: when `lhs` is statically a `TypedColumn[_, _, N]`, the macro bakes `"<name>" <op> $1` as a
 * compile-time-constant `Fragment[S]` and applies `rhs` directly. For any other LHS shape, the macro renders
 * the LHS to AppliedFragment and uses `Encoder.contramap` to bake those args while still surfacing
 * `Where[Stripped[T]]` at the call site.
 */
private inline def valOp[T](inline op: String, inline lhs: TypedExpr[T], rhs: Stripped[T])(using
  pf: PgTypeFor[Stripped[T]]
): Where[Stripped[T]] =
  SqlMacros.infix[T, Stripped[T]](op, lhs, rhs)

/**
 * Infix comparison between two pre-built expressions. Used by the `====` / `!==` column-vs-column overloads.
 * `inline` so the literal op string (`"="` / `"<>"`) splices into `TypedExpr.raw(" = ")` at the call site —
 * the `raw` macro then interns the resulting compile-time-constant string and the comparison allocates no
 * dynamic AppliedFragment per use.
 */
private inline def exprOp[T](inline op: String, lhs: TypedExpr[T], rhs: TypedExpr[T]): Where[skunk.Void] = {
  val opAf = TypedExpr.raw(" " + op + " ")
  Where.fromTypedExpr(TypedExpr(lhs.render |+| opAf |+| rhs.render, skunk.codec.all.bool))
}

/** Equality: `lhs = rhs`. */
extension [T](inline lhs: TypedExpr[T])
  inline def ===(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where[Stripped[T]] = valOp("=", lhs, rhs)

/** Inequality: `lhs <> rhs`. */
extension [T](inline lhs: TypedExpr[T])
  inline def !==(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where[Stripped[T]] = valOp("<>", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def <(rhs: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[Stripped[T]] =
    valOp("<", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def <=(rhs: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[Stripped[T]] =
    valOp("<=", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def >(rhs: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[Stripped[T]] =
    valOp(">", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def >=(rhs: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[Stripped[T]] =
    valOp(">=", lhs, rhs)

extension [T](lhs: TypedExpr[T]) {

  /** Column-to-expression equality. RHS is another typed expression (column, function call, etc.). */
  inline def ====(rhs: TypedExpr[T]): Where[skunk.Void]   = exprOp("=", lhs, rhs)
  inline def `!==`(rhs: TypedExpr[T]): Where[skunk.Void]  = exprOp("<>", lhs, rhs)
}

extension [T](inline lhs: TypedExpr[T])
  /** `lhs BETWEEN lo AND hi` — inclusive on both ends. */
  inline def between(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf:          PgTypeFor[Stripped[T]]
  ): Where[(Stripped[T], Stripped[T])] =
    SqlMacros.infix2[T, Stripped[T]]("BETWEEN", "AND", lhs, lo, hi)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs NOT BETWEEN lo AND hi`. */
  inline def notBetween(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf:          PgTypeFor[Stripped[T]]
  ): Where[(Stripped[T], Stripped[T])] =
    SqlMacros.infix2[T, Stripped[T]]("NOT BETWEEN", "AND", lhs, lo, hi)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs BETWEEN SYMMETRIC lo AND hi` — Postgres form that auto-swaps `lo` and `hi` if `lo > hi`. */
  inline def betweenSymmetric(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf:          PgTypeFor[Stripped[T]]
  ): Where[(Stripped[T], Stripped[T])] =
    SqlMacros.infix2[T, Stripped[T]]("BETWEEN SYMMETRIC", "AND", lhs, lo, hi)

/**
 * ANY / ALL quantifier over a subquery RHS. Renders as `<lhs> <op> ANY (<subquery>)` or `<lhs> <op> ALL
 * (<subquery>)`. The inner subquery's args are baked via contramap into the resulting `Where[Void]`'s encoder.
 */
private def quantifiedRender[T, Q, ET](
  lhs: TypedExpr[T],
  op: String,
  quant: String,
  q: Q
)(using ev: skunk.sharp.dsl.AsSubquery[Q, ET]): Where[skunk.Void] = {
  val rendered = ev.render(q)()
  val combined =
    lhs.render |+|
      TypedExpr.raw(s" $op $quant (") |+|
      rendered |+|
      TypedExpr.raw(")")
  Where.fromTypedExpr(TypedExpr(combined, skunk.codec.all.bool))
}

extension [T](lhs: TypedExpr[T])(using @unused ord: cats.Order[Stripped[T]]) {

  /** `lhs < ANY (subquery)`. */
  def ltAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<", "ANY", q)

  /** `lhs <= ANY (subquery)`. */
  def lteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<=", "ANY", q)

  /** `lhs > ANY (subquery)`. */
  def gtAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">", "ANY", q)

  /** `lhs >= ANY (subquery)`. */
  def gteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">=", "ANY", q)

  /** `lhs < ALL (subquery)`. */
  def ltAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<", "ALL", q)

  /** `lhs <= ALL (subquery)`. */
  def lteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<=", "ALL", q)

  /** `lhs > ALL (subquery)`. */
  def gtAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">", "ALL", q)

  /** `lhs >= ALL (subquery)`. */
  def gteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[skunk.Void] =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">=", "ALL", q)

}

extension [T](inline lhs: TypedExpr[T])
  /** `lhs IS DISTINCT FROM rhs` — NULL-safe inequality. */
  inline def isDistinctFrom(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where[Stripped[T]] =
    valOp("IS DISTINCT FROM", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs IS NOT DISTINCT FROM rhs` — NULL-safe equality. */
  inline def isNotDistinctFrom(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where[Stripped[T]] =
    valOp("IS NOT DISTINCT FROM", lhs, rhs)

extension [T](lhs: TypedExpr[T]) {

  /** Expression-to-expression `IS DISTINCT FROM` — for column-vs-column / column-vs-function-call. */
  def isDistinctFromExpr(rhs: TypedExpr[T]): Where[skunk.Void] =
    Where.fromTypedExpr(PgOperator.infix[T, T, Boolean]("IS DISTINCT FROM")(lhs, rhs))

  /** Expression-to-expression `IS NOT DISTINCT FROM`. */
  def isNotDistinctFromExpr(rhs: TypedExpr[T]): Where[skunk.Void] =
    Where.fromTypedExpr(PgOperator.infix[T, T, Boolean]("IS NOT DISTINCT FROM")(lhs, rhs))

}

/**
 * Evidence that `Rhs` can sit on the right-hand side of `lhs IN (...)`. Two givens:
 *
 *   - A `Reducible` container of values → renders as `(lit1, lit2, …)` with each value bound as a parameter.
 *   - A subquery → renders as `(<subquery>)`. Correlation is automatic when the subquery is built inside an
 *     outer `.where` lambda.
 */
sealed trait InRhs[T, Rhs] {
  def renderParens(rhs: Rhs): skunk.AppliedFragment
}

object InRhs {

  given reducibleIn[T, F[_]](using R: cats.Reducible[F], pf: PgTypeFor[T]): InRhs[T, F[T]] =
    new InRhs[T, F[T]] {
      def renderParens(values: F[T]): skunk.AppliedFragment = {
        val literals = R.toNonEmptyList(values).toList.map(v => TypedExpr.parameterised(v).render)
        TypedExpr.raw("(") |+| TypedExpr.joined(literals, ", ") |+| TypedExpr.raw(")")
      }
    }

  given subqueryIn[T, Q](using ev: skunk.sharp.dsl.AsSubquery[Q, T]): InRhs[T, Q] =
    new InRhs[T, Q] {
      def renderParens(q: Q): skunk.AppliedFragment = {
        val cq = ev.toCompiled(q)
        TypedExpr.raw("(") |+| cq.af |+| TypedExpr.raw(")")
      }
    }

}

extension [T](inline lhs: TypedExpr[T])
  /**
   * `lhs IN (...)`. Produces `Where[Void]` — the bound parameters from the literal list or subquery are baked
   * into the AppliedFragment-level encoder via the lift through `Where.fromTypedExpr`.
   */
  inline def in[Rhs](rhs: Rhs)(using ev: InRhs[Stripped[T], Rhs]): Where[skunk.Void] =
    Where.fromTypedExpr(
      TypedExpr(
        SqlMacros.prefix[T](lhs, "IN") |+| ev.renderParens(rhs),
        skunk.codec.all.bool
      )
    )

extension [T](inline lhs: TypedExpr[T])
  /** `lhs LIKE pattern`. */
  inline def like(pattern: String)(using
    @unused ev: Stripped[T] <:< String,
    pf: PgTypeFor[String]
  ): Where[String] =
    SqlMacros.infix[T, String]("LIKE", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs ILIKE pattern` (case-insensitive). */
  inline def ilike(pattern: String)(using
    @unused ev: Stripped[T] <:< String,
    pf: PgTypeFor[String]
  ): Where[String] =
    SqlMacros.infix[T, String]("ILIKE", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs SIMILAR TO pattern` — Postgres's SQL-standard regex variant. */
  inline def similarTo(pattern: String)(using
    @unused ev: Stripped[T] <:< String,
    pf: PgTypeFor[String]
  ): Where[String] =
    SqlMacros.infix[T, String]("SIMILAR TO", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs NOT SIMILAR TO pattern`. */
  inline def notSimilarTo(pattern: String)(using
    @unused ev: Stripped[T] <:< String,
    pf: PgTypeFor[String]
  ): Where[String] =
    SqlMacros.infix[T, String]("NOT SIMILAR TO", lhs, pattern)

extension [T, Null <: Boolean, N <: String & Singleton](inline lhs: skunk.sharp.TypedColumn[T, Null, N]) {

  /** `lhs IS NULL`. Compiles only for nullable columns. */
  inline def isNull: Where[skunk.Void] = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNull` is only available on nullable columns (columns declared as `Option[_]`).")
    SqlMacros.postfix[T](lhs, " IS NULL")
  }

  /** `lhs IS NOT NULL`. Compiles only for nullable columns. */
  inline def isNotNull: Where[skunk.Void] = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNotNull` is only available on nullable columns (columns declared as `Option[_]`).")
    SqlMacros.postfix[T](lhs, " IS NOT NULL")
  }

}
