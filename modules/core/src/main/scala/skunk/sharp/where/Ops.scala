package skunk.sharp.where

import skunk.AppliedFragment
import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor

import scala.annotation.unused

/**
 * The v0 WHERE operator set: `=, <>, <, <=, >, >=, IN, LIKE, IS NULL` plus logical combinators (on `Where`).
 *
 * Operators are *extension methods* on `TypedExpr[T]` so third-party modules add new ones without touching core (jsonb
 * `->>`, ltree `~`, arrays `@>`, …).
 *
 * **Nullable columns.** If a column is declared nullable, comparisons like `col === value` take the underlying value
 * type, not `Option[value]`. Trying to compare against `None` is a compile error — use `.isNull` / `.isNotNull`
 * instead, since in SQL `col = NULL` is never true (three-valued logic). See [[Stripped]].
 */

/**
 * Type-level alias: stripped of the outermost `Option[_]` if there is one, otherwise unchanged. Drives the RHS of
 * comparison operators so callers pass the underlying value even for nullable columns.
 */
type Stripped[T] = T match {
  case Option[x] => x
  case _         => T
}

/** Common binary operator rendering: `<lhs> <op> <rhs>`. */
private def binOp(op: String, l: TypedExpr[?], r: TypedExpr[?]): Where = {
  val af: AppliedFragment = l.render |+| TypedExpr.raw(s" $op ") |+| r.render
  Where(new TypedExpr[Boolean] {
    val render = af
    val codec  = skunk.codec.all.bool
  })
}

extension [T](lhs: TypedExpr[T]) {

  /**
   * Equality: `lhs = rhs`. For nullable `TypedExpr[Option[X]]`, `rhs` must be an `X` — comparisons with `None` are a
   * compile error (use `.isNull` instead).
   */
  def ===(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where = binOp("=", lhs, TypedExpr.lit(rhs))

  /** Inequality: `lhs <> rhs`. */
  def !==(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where = binOp("<>", lhs, TypedExpr.lit(rhs))

  /**
   * Column-to-expression equality for when the RHS is another typed expression (another column, a function call, …).
   * Kept as a separate method name because mixing literal- and expression-RHS overloads of `===` in Scala 3 confuses
   * extension-method resolution.
   */
  def ====(rhs: TypedExpr[T]): Where  = binOp("=", lhs, rhs)
  def `!==`(rhs: TypedExpr[T]): Where = binOp("<>", lhs, rhs)
}

extension [T](lhs: TypedExpr[T])(using @unused ord: cats.Order[Stripped[T]]) {

  def <(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where  = binOp("<", lhs, TypedExpr.lit(rhs))
  def <=(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where = binOp("<=", lhs, TypedExpr.lit(rhs))
  def >(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where  = binOp(">", lhs, TypedExpr.lit(rhs))
  def >=(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where = binOp(">=", lhs, TypedExpr.lit(rhs))
}

/**
 * Evidence that `Rhs` can sit on the right-hand side of `lhs IN (...)`. Ships two givens:
 *
 *   - A `Reducible` container of values (`NonEmptyList[T]`, `NonEmptyVector[T]`, …) → renders as
 *     `(lit1, lit2, …)` with each value bound as a parameter.
 *   - A [[skunk.sharp.dsl.CompiledQuery]]`[T]` → renders as `(<subquery>)`. Correlation is automatic when the
 *     subquery is built inside an outer `.where` / `.select` lambda — outer [[skunk.sharp.TypedColumn]]s render
 *     alias-qualified and reference the outer source.
 *
 * Unified behind one `.in` extension to avoid Scala 3 overload-resolution pitfalls when re-exported from the `dsl`
 * package object.
 */
sealed trait InRhs[T, Rhs] {
  def renderParens(rhs: Rhs): skunk.AppliedFragment
}

object InRhs {

  given reducibleIn[T, F[_]](using R: cats.Reducible[F], pf: PgTypeFor[T]): InRhs[T, F[T]] =
    new InRhs[T, F[T]] {
      def renderParens(values: F[T]): skunk.AppliedFragment = {
        val literals = R.toNonEmptyList(values).toList.map(v => TypedExpr.lit(v).render)
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

extension [T](lhs: TypedExpr[T]) {

  /**
   * `lhs IN (...)`. The right-hand side is anything with an [[InRhs]] instance — a `Reducible` container of values
   * or a [[skunk.sharp.dsl.CompiledQuery]] for subquery IN.
   */
  def in[Rhs](rhs: Rhs)(using ev: InRhs[Stripped[T], Rhs]): Where = {
    val rendered = lhs.render |+| TypedExpr.raw(" IN ") |+| ev.renderParens(rhs)
    Where(new TypedExpr[Boolean] {
      val render = rendered
      val codec  = skunk.codec.all.bool
    })
  }

}

extension [T](lhs: TypedExpr[T])(using @unused ev: Stripped[T] =:= String) {

  /** `lhs LIKE pattern`. Works on both `TypedExpr[String]` and `TypedExpr[Option[String]]`. */
  def like(pattern: String): Where = binOp("LIKE", lhs, TypedExpr.lit(pattern))

  /** `lhs ILIKE pattern` (case-insensitive). */
  def ilike(pattern: String): Where = binOp("ILIKE", lhs, TypedExpr.lit(pattern))
}

extension [T, Null <: Boolean](lhs: skunk.sharp.TypedColumn[T, Null]) {

  /** `lhs IS NULL`. Compiles only for nullable columns — non-nullable columns get a friendly compile error. */
  inline def isNull: Where = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNull` is only available on nullable columns (columns declared as `Option[_]`).")
    nullCheck(lhs, " IS NULL")
  }

  /** `lhs IS NOT NULL`. Compiles only for nullable columns. */
  inline def isNotNull: Where = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNotNull` is only available on nullable columns (columns declared as `Option[_]`).")
    nullCheck(lhs, " IS NOT NULL")
  }

}

private def nullCheck(col: skunk.sharp.TypedColumn[?, ?], suffix: String): Where = {
  val af = col.render |+| TypedExpr.raw(suffix)
  Where(new TypedExpr[Boolean] {
    val render = af
    val codec  = skunk.codec.all.bool
  })
}
