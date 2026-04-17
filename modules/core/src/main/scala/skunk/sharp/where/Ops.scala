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

extension [T](lhs: TypedExpr[T]) {

  /** `lhs IN (val1, val2, …)`. Empty input compiles to `FALSE`. */
  def in(values: Seq[Stripped[T]])(using pf: PgTypeFor[Stripped[T]]): Where =
    if values.isEmpty then Where(falseExpr)
    else {
      val literals = values.map(v => TypedExpr.lit(v).render).toList
      val rendered =
        lhs.render |+| TypedExpr.raw(" IN (") |+| TypedExpr.joined(literals, ", ") |+| TypedExpr.raw(")")
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

private lazy val falseExpr: TypedExpr[Boolean] =
  new TypedExpr[Boolean] {
    val render = TypedExpr.raw("FALSE")
    val codec  = skunk.codec.all.bool
  }
