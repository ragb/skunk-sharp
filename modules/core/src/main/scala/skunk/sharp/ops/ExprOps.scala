package skunk.sharp.ops

import skunk.{Encoder, Fragment, Void}
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.util.Origin

import scala.annotation.unused

/**
 * The v0 expression-level operator set: `=, <>, <, <=, >, >=, BETWEEN, IN, LIKE, IS NULL`. Each operator produces
 * a `Where[A]` (= `TypedExpr[Boolean, A]`) — a typed predicate carrying its parameter tuple as a visible Args
 * type. Operators slot wherever a boolean expression is valid in Postgres: WHERE, HAVING, SELECT projections,
 * ORDER BY, function arguments, CASE WHEN predicates.
 *
 * Operators are *extension methods* on `TypedExpr[T, A]` so third-party modules add new ones without touching
 * core.
 *
 * **RHS forms** for binary operators:
 *
 *   - `lhs === Param[T]` — deferred parameter, supplied at execute time. Args contributes `T`.
 *   - `lhs === lit(v)` — compile-time literal, inline SQL. Args contributes `Void`.
 *   - `lhs === otherExpr` — column-vs-expression / function-call result. Args from `otherExpr`.
 *   - `lhs === runtimeValue` — runtime value baked via [[Param.bind]] into a Void-args fragment. Args = Void.
 *     Convenient for ad-hoc queries; loses Skunk plan-cache benefits compared to the `Param[T]` form.
 *
 * **Nullable columns.** If a column is declared nullable, comparisons like `col === value` take the underlying
 * value type, not `Option[value]`. Trying to compare against `None` is a compile error — use `.isNull` /
 * `.isNotNull` instead. See [[Stripped]].
 */

/**
 * Type-level alias: stripped of the outermost `Option[_]` if there is one, otherwise unchanged. Drives the RHS
 * of comparison operators so callers pass the underlying value even for nullable columns.
 */
type Stripped[T] = T match {
  case Option[x] => x
  case _         => T
}

/** Build a `Where[Concat[A, B]]` from `lhs <op> rhs`. Both arms are typed expressions; Args from each propagate. */
private def opCombine[T, U, A, B](
  lhs: TypedExpr[T, A],
  opSql: String,
  rhs: TypedExpr[U, B]
): Where[Where.Concat[A, B]] = {
  val frag = TypedExpr.combineSep(lhs.fragment, opSql, rhs.fragment)
  Where(frag)
}

/** Equality: `lhs = rhs`. */
extension [T, A](lhs: TypedExpr[T, A]) {

  /** Equality with another typed expression / Param / literal. */
  def ===[B](rhs: TypedExpr[Stripped[T], B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)

  /** Equality with a runtime value (bound as `$N` via [[Param.bind]]). */
  def ===(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " = ", Param.bind(rhs)).asInstanceOf[Where[A]]

  /** Inequality: `lhs <> rhs`. */
  def !==[B](rhs: TypedExpr[Stripped[T], B]): Where[Where.Concat[A, B]] = opCombine(lhs, " <> ", rhs)

  /** Inequality with a runtime value. */
  def !==(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " <> ", Param.bind(rhs)).asInstanceOf[Where[A]]

  /** `lhs < rhs`. */
  def <[B](rhs: TypedExpr[Stripped[T], B])(using @unused ord: cats.Order[Stripped[T]]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " < ", rhs)

  def <(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " < ", Param.bind(rhs)).asInstanceOf[Where[A]]

  /** `lhs <= rhs`. */
  def <=[B](rhs: TypedExpr[Stripped[T], B])(using @unused ord: cats.Order[Stripped[T]]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " <= ", rhs)

  def <=(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " <= ", Param.bind(rhs)).asInstanceOf[Where[A]]

  /** `lhs > rhs`. */
  def >[B](rhs: TypedExpr[Stripped[T], B])(using @unused ord: cats.Order[Stripped[T]]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " > ", rhs)

  def >(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " > ", Param.bind(rhs)).asInstanceOf[Where[A]]

  /** `lhs >= rhs`. */
  def >=[B](rhs: TypedExpr[Stripped[T], B])(using @unused ord: cats.Order[Stripped[T]]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " >= ", rhs)

  def >=(rhs: Stripped[T])(using @unused ord: cats.Order[Stripped[T]], pf: PgTypeFor[Stripped[T]]): Where[A] =
    opCombine(lhs, " >= ", Param.bind(rhs)).asInstanceOf[Where[A]]

}

/** Column-to-expression equality alias for source compat. Equivalent to `===` with TypedExpr RHS. */
extension [T, A](lhs: TypedExpr[T, A]) {

  /** Same as `===` with TypedExpr RHS — column-vs-column / column-vs-function-call. */
  def ====[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)

}

/** `lhs BETWEEN lo AND hi` family. */
extension [T, A](lhs: TypedExpr[T, A]) {

  def between[B, C](lo: TypedExpr[Stripped[T], B], hi: TypedExpr[Stripped[T], C])(using
    @unused ord: cats.Order[Stripped[T]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN ", TypedExpr[Stripped[T], Where.Concat[B, C]](rhs, lo.codec))
  }

  def between(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[A] =
    between(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

  def notBetween[B, C](lo: TypedExpr[Stripped[T], B], hi: TypedExpr[Stripped[T], C])(using
    @unused ord: cats.Order[Stripped[T]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " NOT BETWEEN ", TypedExpr[Stripped[T], Where.Concat[B, C]](rhs, lo.codec))
  }

  def notBetween(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[A] =
    notBetween(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

  def betweenSymmetric[B, C](lo: TypedExpr[Stripped[T], B], hi: TypedExpr[Stripped[T], C])(using
    @unused ord: cats.Order[Stripped[T]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN SYMMETRIC ", TypedExpr[Stripped[T], Where.Concat[B, C]](rhs, lo.codec))
  }

  def betweenSymmetric(lo: Stripped[T], hi: Stripped[T])(using
    @unused ord: cats.Order[Stripped[T]],
    pf: PgTypeFor[Stripped[T]]
  ): Where[A] =
    betweenSymmetric(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

}

/** `lhs IS DISTINCT FROM rhs` / `lhs IS NOT DISTINCT FROM rhs` — NULL-safe (in)equality. */
extension [T, A](lhs: TypedExpr[T, A]) {

  def isDistinctFrom[B](rhs: TypedExpr[Stripped[T], B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS DISTINCT FROM ", rhs)

  def isDistinctFrom(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where[A] =
    isDistinctFrom(Param.bind(rhs)).asInstanceOf[Where[A]]

  def isNotDistinctFrom[B](rhs: TypedExpr[Stripped[T], B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS NOT DISTINCT FROM ", rhs)

  def isNotDistinctFrom(rhs: Stripped[T])(using pf: PgTypeFor[Stripped[T]]): Where[A] =
    isNotDistinctFrom(Param.bind(rhs)).asInstanceOf[Where[A]]

  /** Source-compat aliases for the column-vs-column NULL-safe variants. */
  def isDistinctFromExpr[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS DISTINCT FROM ", rhs)

  def isNotDistinctFromExpr[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS NOT DISTINCT FROM ", rhs)

}

/** `lhs LIKE pattern` / `ILIKE` / `SIMILAR TO`. */
extension [T, A](lhs: TypedExpr[T, A]) {

  def like[B](pattern: TypedExpr[String, B])(using @unused ev: Stripped[T] <:< String): Where[Where.Concat[A, B]] =
    opCombine(lhs, " LIKE ", pattern)

  def like(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where[A] =
    like(Param.bind(pattern)).asInstanceOf[Where[A]]

  def ilike[B](pattern: TypedExpr[String, B])(using @unused ev: Stripped[T] <:< String): Where[Where.Concat[A, B]] =
    opCombine(lhs, " ILIKE ", pattern)

  def ilike(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where[A] =
    ilike(Param.bind(pattern)).asInstanceOf[Where[A]]

  def similarTo[B](pattern: TypedExpr[String, B])(using @unused ev: Stripped[T] <:< String): Where[Where.Concat[A, B]] =
    opCombine(lhs, " SIMILAR TO ", pattern)

  def similarTo(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where[A] =
    similarTo(Param.bind(pattern)).asInstanceOf[Where[A]]

  def notSimilarTo[B](pattern: TypedExpr[String, B])(using @unused ev: Stripped[T] <:< String): Where[Where.Concat[A, B]] =
    opCombine(lhs, " NOT SIMILAR TO ", pattern)

  def notSimilarTo(pattern: String)(using @unused ev: Stripped[T] <:< String, pf: PgTypeFor[String]): Where[A] =
    notSimilarTo(Param.bind(pattern)).asInstanceOf[Where[A]]

}

/** `lhs IS NULL` / `IS NOT NULL` — compile-only on nullable columns. */
extension [T, Null <: Boolean, N <: String & Singleton](inline lhs: skunk.sharp.TypedColumn[T, Null, N]) {

  inline def isNull: Where[Void] = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNull` is only available on nullable columns (columns declared as `Option[_]`).")
    val parts = lhs.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(" IS NULL"))
    val frag: Fragment[Void] = Fragment(parts, Void.codec, Origin.unknown)
    Where(frag)
  }

  inline def isNotNull: Where[Void] = {
    inline if scala.compiletime.constValue[Null] then ()
    else scala.compiletime.error("`isNotNull` is only available on nullable columns (columns declared as `Option[_]`).")
    val parts = lhs.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(" IS NOT NULL"))
    val frag: Fragment[Void] = Fragment(parts, Void.codec, Origin.unknown)
    Where(frag)
  }

}

/**
 * `lhs IN (values...)` / `lhs IN (subquery)`. The RHS evidence builds a Void-args parenthesised fragment; the
 * LHS's args propagate through.
 */
sealed trait InRhs[T, Rhs] {
  def renderParens(rhs: Rhs): Fragment[Void]
}

object InRhs {

  given reducibleIn[T, F[_]](using R: cats.Reducible[F], pf: PgTypeFor[T]): InRhs[T, F[T]] =
    new InRhs[T, F[T]] {
      def renderParens(values: F[T]): Fragment[Void] = {
        val literals = R.toNonEmptyList(values).toList.map(v => Param.bind[T](v).fragment)
        // Combine all literal fragments via combineSep with ", " separator; wrap in parens.
        val joined = literals.reduceLeft((a, b) => TypedExpr.combineSep(a, ", ", b).asInstanceOf[Fragment[Void]])
        TypedExpr.wrap("(", joined, ")")
      }
    }

  given subqueryIn[T, Q](using ev: skunk.sharp.dsl.AsSubquery[Q, T]): InRhs[T, Q] =
    new InRhs[T, Q] {
      def renderParens(q: Q): Fragment[Void] = {
        val af = ev.render(q)()
        val voidFrag = TypedExpr.liftAfToVoid(af)
        TypedExpr.wrap("(", voidFrag, ")")
      }
    }

}

extension [T, A](lhs: TypedExpr[T, A]) {

  /** `lhs IN (...)`. RHS bound parameters are baked into the encoder; result Args = LHS Args. */
  def in[Rhs](rhs: Rhs)(using ev: InRhs[Stripped[T], Rhs]): Where[A] = {
    val rhsFrag = ev.renderParens(rhs)
    val combined = TypedExpr.combineSep(lhs.fragment, " IN ", rhsFrag)
    Where(combined.asInstanceOf[Fragment[A]])
  }

}

/**
 * ANY / ALL quantifier over a subquery RHS. Renders as `<lhs> <op> ANY (<subquery>)` / `<lhs> <op> ALL
 * (<subquery>)`. The inner subquery's args are baked via contramap so result Args = LHS Args.
 */
private def quantifiedRender[T, A, Q, ET](
  lhs: TypedExpr[T, A],
  op: String,
  quant: String,
  q: Q
)(using ev: skunk.sharp.dsl.AsSubquery[Q, ET]): Where[A] = {
  val af       = ev.render(q)()
  val voidFrag = TypedExpr.liftAfToVoid(af)
  val rhsFrag  = TypedExpr.wrap(s" $op $quant (", voidFrag, ")")
  // Combine LHS with the wrapped RHS — the wrapped RHS already has the operator + paren prefix.
  val combinedParts = lhs.fragment.parts ++ rhsFrag.parts
  val frag: Fragment[A] = Fragment(combinedParts, lhs.fragment.encoder, Origin.unknown)
  Where(frag)
}

extension [T, A](lhs: TypedExpr[T, A])(using @unused ord: cats.Order[Stripped[T]]) {

  def ltAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A]  = quantifiedRender(lhs, "<",  "ANY", q)
  def lteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A] = quantifiedRender(lhs, "<=", "ANY", q)
  def gtAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A]  = quantifiedRender(lhs, ">",  "ANY", q)
  def gteAny[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A] = quantifiedRender(lhs, ">=", "ANY", q)

  def ltAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A]  = quantifiedRender(lhs, "<",  "ALL", q)
  def lteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A] = quantifiedRender(lhs, "<=", "ALL", q)
  def gtAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A]  = quantifiedRender(lhs, ">",  "ALL", q)
  def gteAll[Q](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, Stripped[T]]): Where[A] = quantifiedRender(lhs, ">=", "ALL", q)

}
