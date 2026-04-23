package skunk.sharp.ops

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.internal.SqlMacros
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

import scala.annotation.unused

/**
 * The v0 expression-level operator set: `=, <>, <, <=, >, >=, IN, LIKE, IS NULL`. Every operator produces a
 * `TypedExpr[Boolean]` (aliased as [[skunk.sharp.where.Where]]), which is just a regular expression — it slots anywhere
 * a `TypedExpr[_]` is valid in Postgres: WHERE clauses, SELECT projections (`users.select(u => u.age >= 18)` renders a
 * boolean column), HAVING, ORDER BY, function arguments, CASE WHEN predicates.
 *
 * Lives in `skunk.sharp.ops` (not `.where`) because "WHERE" was misleading — the operators aren't WHERE-specific. The
 * `skunk.sharp.where` package keeps the [[Where]] type alias + logical combinators (`&&`, `||`, unary `!`), which
 * compose booleans regardless of where they end up.
 *
 * Operators are *extension methods* on `TypedExpr[T]` so third-party modules add new ones without touching core (jsonb
 * `->>`, ltree `~`, arrays `@>`, …).
 *
 * **Nullable columns.** If a column is declared nullable, comparisons like `col === value` take the underlying value
 * type, not `Option[value]`. Trying to compare against `None` is a compile error — use `.isNull` / `.isNotNull`
 * instead, since in SQL `col = NULL` is never true (three-valued logic). See [[Stripped]].
 *
 * Rendering: every infix op here delegates to [[PgOperator.infix]] so the string-assembly lives in one place. The only
 * per-op work is picking the SQL symbol and deciding whether the RHS is parameterised (runtime value) or already a
 * [[TypedExpr]] (expression-to-expression compare).
 */

/**
 * Type-level alias: stripped of the outermost `Option[_]` if there is one, otherwise unchanged. Drives the RHS of
 * comparison operators so callers pass the underlying value even for nullable columns.
 */
type Stripped[T] = T match {
  case Option[x] => x
  case _         => T
}

/**
 * Infix comparison with a runtime-parameterised RHS. All the value-on-the-right operators share this shape.
 *
 * `inline` + macro: when `lhs` is statically a `TypedColumn[_, _, N]` (the common case — `u.age`, `u.email`, …),
 * the macro bakes `"<name>" <op> $1` as a compile-time-constant `Fragment` and applies `rhs` directly,
 * skipping the `a.render |+| raw(" op ") |+| b.render` runtime chain. For any other LHS (function calls,
 * casts, subquery `.asExpr`) the macro emits the same `PgOperator.infix` call the hand-written helper would.
 */
private inline def valOp[T](inline op: String, inline lhs: TypedExpr[T], rhs: Stripped[T])(using
  pf: PgTypeFor[Stripped[T]]
): Where =
  SqlMacros.infix[T, Stripped[T]](op, lhs, rhs)

/** Infix comparison between two pre-built expressions. Used by the `====` / `!==` column-vs-column overloads. */
private def exprOp[T](op: String, lhs: TypedExpr[T], rhs: TypedExpr[T]): Where =
  PgOperator.infix[T, T, Boolean](op)(lhs, rhs)

// Each macro-backed operator lives in its own single-method extension so the receiver can be `inline`
// (Scala requires `inline` parameters only on `inline def` methods, which forces one method per extension).

/** Equality: `lhs = rhs`. For nullable `TypedExpr[Option[X]]`, `rhs` must be an `X` — comparisons with `None`
  * are a compile error (use `.isNull` instead).
  */
extension [T](inline lhs: TypedExpr[T])
  inline def ===(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where = valOp("=", lhs, rhs)

/** Inequality: `lhs <> rhs`. */
extension [T](inline lhs: TypedExpr[T])
  inline def !==(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where = valOp("<>", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def <(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where =
    valOp("<", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def <=(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where =
    valOp("<=", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def >(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where =
    valOp(">", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  inline def >=(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where =
    valOp(">=", lhs, rhs)

// Non-macro operators keep the traditional multi-method extension shape.

extension [T](lhs: TypedExpr[T]) {

  /**
   * Column-to-expression equality for when the RHS is another typed expression (another column, a function call, …).
   * Kept as a separate method name because mixing literal- and expression-RHS overloads of `===` in Scala 3 confuses
   * extension-method resolution.
   */
  def ====(rhs: TypedExpr[T]): Where  = exprOp("=", lhs, rhs)
  def `!==`(rhs: TypedExpr[T]): Where = exprOp("<>", lhs, rhs)
}

extension [T](lhs: TypedExpr[T])(using @unused ord: cats.Order[Stripped[T]]) {

  /**
   * `lhs BETWEEN lo AND hi` — inclusive on both ends. Values on the RHS are runtime-parameterised (two `$N`s), so this
   * is distinct from the `col >= lo AND col <= hi` form in SQL surface only — Postgres's planner treats them
   * identically, but users expect the keyword form.
   */
  def between(lo: Stripped[T], hi: Stripped[T])(using PgTypeFor[Stripped[T]]): Where =
    betweenRender(lhs, "BETWEEN", lo, hi)

  /** `lhs NOT BETWEEN lo AND hi`. Exclusive complement. */
  def notBetween(lo: Stripped[T], hi: Stripped[T])(using PgTypeFor[Stripped[T]]): Where =
    betweenRender(lhs, "NOT BETWEEN", lo, hi)

  /**
   * `lhs BETWEEN SYMMETRIC lo AND hi` — Postgres form that auto-swaps `lo` and `hi` if `lo > hi`. Useful when the
   * bounds come from user input and their order is not guaranteed.
   */
  def betweenSymmetric(lo: Stripped[T], hi: Stripped[T])(using PgTypeFor[Stripped[T]]): Where =
    betweenRender(lhs, "BETWEEN SYMMETRIC", lo, hi)

}

private def betweenRender[T](
  lhs: TypedExpr[T],
  kw: String,
  lo: Stripped[T],
  hi: Stripped[T]
)(using pf: PgTypeFor[Stripped[T]]): Where =
  new TypedExpr[Boolean] {
    val render =
      lhs.render |+|
        TypedExpr.raw(s" $kw ") |+|
        TypedExpr.parameterised(lo).render |+|
        TypedExpr.raw(" AND ") |+|
        TypedExpr.parameterised(hi).render
    val codec = skunk.codec.all.bool
  }

/**
 * ANY / ALL quantifier over a subquery RHS. Renders as `<lhs> <op> ANY (<subquery>)` or `<lhs> <op> ALL (<subquery>)`.
 *
 *   - `ANY` is true iff the comparison holds for *at least one* row.
 *   - `ALL` is true iff the comparison holds for *every* row (and vacuously true for an empty subquery).
 *
 * `<op>` is any of `<`, `<=`, `>`, `>=` — the ordering forms are the interesting ones. `= ANY` is synonymous with
 * `col IN (subquery)`, already reachable via [[in]]. `<> ALL` is synonymous with `NOT IN`.
 */
private def quantifiedRender[T, Q, ET](
  lhs: TypedExpr[T],
  op: String,
  quant: String,
  q: Q
)(using ev: skunk.sharp.dsl.AsSubquery[Q, ET]): Where = {
  val rendered = ev.render(q)
  new TypedExpr[Boolean] {
    val render =
      lhs.render |+|
        TypedExpr.raw(s" $op $quant (") |+|
        rendered() |+|
        TypedExpr.raw(")")
    val codec = skunk.codec.all.bool
  }
}

extension [T](lhs: TypedExpr[T])(using @unused ord: cats.Order[Stripped[T]]) {

  /** `lhs < ANY (subquery)` — true if `lhs` is strictly less than at least one subquery element. */
  def ltAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<", "ANY", q)

  /** `lhs <= ANY (subquery)`. */
  def lteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<=", "ANY", q)

  /** `lhs > ANY (subquery)`. */
  def gtAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">", "ANY", q)

  /** `lhs >= ANY (subquery)`. */
  def gteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">=", "ANY", q)

  /** `lhs < ALL (subquery)` — true if `lhs` is strictly less than every subquery element (vacuously for empty). */
  def ltAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<", "ALL", q)

  /** `lhs <= ALL (subquery)`. */
  def lteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, "<=", "ALL", q)

  /** `lhs > ALL (subquery)`. */
  def gtAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">", "ALL", q)

  /** `lhs >= ALL (subquery)`. */
  def gteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where =
    quantifiedRender[T, Q, Stripped[T]](lhs, ">=", "ALL", q)

}

extension [T](inline lhs: TypedExpr[T])
  /**
   * `lhs IS DISTINCT FROM rhs` — NULL-safe inequality. Unlike `<>`, treats NULL as an ordinary value: `NULL IS DISTINCT
   * FROM 1` is TRUE, `NULL IS DISTINCT FROM NULL` is FALSE. Use on nullable columns when you want "values differ
   * (including NULL vs. not-NULL)" rather than three-valued-logic inequality.
   */
  inline def isDistinctFrom(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where =
    valOp("IS DISTINCT FROM", lhs, rhs)

extension [T](inline lhs: TypedExpr[T])
  /**
   * `lhs IS NOT DISTINCT FROM rhs` — NULL-safe equality. `NULL IS NOT DISTINCT FROM NULL` is TRUE; `NULL IS NOT
   * DISTINCT FROM 1` is FALSE. Dual of `isDistinctFrom`.
   */
  inline def isNotDistinctFrom(rhs: Stripped[T])(using PgTypeFor[Stripped[T]]): Where =
    valOp("IS NOT DISTINCT FROM", lhs, rhs)

extension [T](lhs: TypedExpr[T]) {

  /** Expression-to-expression `IS DISTINCT FROM` — for column-vs-column / column-vs-function-call comparisons. */
  def isDistinctFromExpr(rhs: TypedExpr[T]): Where =
    PgOperator.infix[T, T, Boolean]("IS DISTINCT FROM")(lhs, rhs)

  /** Expression-to-expression `IS NOT DISTINCT FROM`. */
  def isNotDistinctFromExpr(rhs: TypedExpr[T]): Where =
    PgOperator.infix[T, T, Boolean]("IS NOT DISTINCT FROM")(lhs, rhs)

}

/**
 * Evidence that `Rhs` can sit on the right-hand side of `lhs IN (...)`. Ships two givens:
 *
 *   - A `Reducible` container of values (`NonEmptyList[T]`, `NonEmptyVector[T]`, …) → renders as `(lit1, lit2, …)` with
 *     each value bound as a parameter.
 *   - A [[skunk.sharp.dsl.CompiledQuery]]`[T]` → renders as `(<subquery>)`. Correlation is automatic when the subquery
 *     is built inside an outer `.where` / `.select` lambda — outer [[skunk.sharp.TypedColumn]]s render alias-qualified
 *     and reference the outer source.
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

extension [T](lhs: TypedExpr[T]) {

  /**
   * `lhs IN (...)`. The right-hand side is anything with an [[InRhs]] instance — a `Reducible` container of values or a
   * [[skunk.sharp.dsl.CompiledQuery]] for subquery IN.
   */
  def in[Rhs](rhs: Rhs)(using ev: InRhs[Stripped[T], Rhs]): Where =
    new TypedExpr[Boolean] {
      val render = lhs.render |+| TypedExpr.raw(" IN ") |+| ev.renderParens(rhs)
      val codec  = skunk.codec.all.bool
    }

}

extension [T](inline lhs: TypedExpr[T])
  /**
   * `lhs LIKE pattern`. Works on any string-like column — `TypedExpr[String]`, tag types (`TypedExpr[Varchar[N]]`,
   * `TypedExpr[Bpchar[N]]`, `TypedExpr[Text]`), and their `Option` variants.
   */
  inline def like(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where =
    SqlMacros.infix[T, String]("LIKE", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs ILIKE pattern` (case-insensitive). */
  inline def ilike(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where =
    SqlMacros.infix[T, String]("ILIKE", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /**
   * `lhs SIMILAR TO pattern` — Postgres's SQL-standard regex variant. Syntax lies between `LIKE` and POSIX regex:
   * supports `_` / `%` wildcards plus regex-style `|`, `*`, `+`, `?`, `()`, `[]`. Less common than `~` / `~*` but part
   * of the standard.
   */
  inline def similarTo(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where =
    SqlMacros.infix[T, String]("SIMILAR TO", lhs, pattern)

extension [T](inline lhs: TypedExpr[T])
  /** `lhs NOT SIMILAR TO pattern`. */
  inline def notSimilarTo(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where =
    SqlMacros.infix[T, String]("NOT SIMILAR TO", lhs, pattern)

extension [T, Null <: Boolean, N <: String & Singleton](inline lhs: skunk.sharp.TypedColumn[T, Null, N]) {

  /** `lhs IS NULL`. Compiles only for nullable columns — non-nullable columns get a friendly compile error. */
  inline def isNull: Where = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNull` is only available on nullable columns (columns declared as `Option[_]`).")
    SqlMacros.postfix[T](lhs, " IS NULL")
  }

  /** `lhs IS NOT NULL`. Compiles only for nullable columns. */
  inline def isNotNull: Where = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNotNull` is only available on nullable columns (columns declared as `Option[_]`).")
    SqlMacros.postfix[T](lhs, " IS NOT NULL")
  }

}
