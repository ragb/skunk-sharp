package skunk.sharp.ops

import skunk.{Fragment, Void}
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
 * Type-level alias: strip outermost `Option[_]` if there is one, otherwise unchanged. **Not** used in the
 * comparison operators below — overloaded extensions whose parameters mention a match type confuse Scala 3's
 * resolution machinery and the value-overload silently fails to apply. Comparison operators take `T` directly
 * for the value-RHS form; nullable-column cases pass `Some(value)` / `None` (or use `.isNull` / `.isNotNull`).
 * Kept exported for source-compat with code that referenced the alias.
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
)(using c2: Where.Concat2[A, B]): Where[Where.Concat[A, B]] = {
  val frag = TypedExpr.combineSep(lhs.fragment, opSql, rhs.fragment)
  Where(frag)
}

// Each operator's typed-RHS form and value-RHS form live in *separate* extension blocks. Putting them in a
// single block confuses Scala 3's overload resolution when one branch has a `using` clause and the match-type
// `T` appears in a parameter position — overload search fails before the `using` is summoned.

extension [T, A](lhs: TypedExpr[T, A]) {
  /** `lhs = rhs` — typed expression / Param / literal RHS. */
  def ===[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  /** `lhs = value` — runtime value baked via [[Param.bind]]. */
  def ===(rhs: T)(using pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " = ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def !==[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " <> ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def !==(rhs: T)(using pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " <> ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def <[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " < ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def <(rhs: T)(using @unused ord: cats.Order[T], pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " < ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def <=[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " <= ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def <=(rhs: T)(using @unused ord: cats.Order[T], pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " <= ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def >[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " > ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def >(rhs: T)(using @unused ord: cats.Order[T], pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " > ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def >=[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " >= ", rhs)
}

extension [T, A](lhs: TypedExpr[T, A]) {
  def >=(rhs: T)(using @unused ord: cats.Order[T], pf: PgTypeFor[T]): Where[A] =
    opCombine(lhs, " >= ", Param.bind(rhs)).asInstanceOf[Where[A]]
}

/** Column-to-expression equality alias for source compat. Equivalent to `===` with TypedExpr RHS. */
extension [T, A](lhs: TypedExpr[T, A]) {

  /** Same as `===` with TypedExpr RHS — column-vs-column / column-vs-function-call. */
  def ====[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)

}

/** `lhs BETWEEN lo AND hi` family. */
extension [T, A](lhs: TypedExpr[T, A]) {

  def between[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T],
    c2_BC: Where.Concat2[B, C],
    c2_AB: Where.Concat2[A, Where.Concat[B, C]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

  def between(lo: T, hi: T)(using
    @unused ord: cats.Order[T],
    pf: PgTypeFor[T]
  ): Where[A] =
    between(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

  def notBetween[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T],
    c2_BC: Where.Concat2[B, C],
    c2_AB: Where.Concat2[A, Where.Concat[B, C]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " NOT BETWEEN ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

  def notBetween(lo: T, hi: T)(using
    @unused ord: cats.Order[T],
    pf: PgTypeFor[T]
  ): Where[A] =
    notBetween(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

  def betweenSymmetric[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T],
    c2_BC: Where.Concat2[B, C],
    c2_AB: Where.Concat2[A, Where.Concat[B, C]]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSep(lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN SYMMETRIC ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

  def betweenSymmetric(lo: T, hi: T)(using
    @unused ord: cats.Order[T],
    pf: PgTypeFor[T]
  ): Where[A] =
    betweenSymmetric(Param.bind(lo), Param.bind(hi)).asInstanceOf[Where[A]]

}

/** `lhs IS DISTINCT FROM rhs` / `lhs IS NOT DISTINCT FROM rhs` — NULL-safe (in)equality. */
extension [T, A](lhs: TypedExpr[T, A]) {

  def isDistinctFrom[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS DISTINCT FROM ", rhs)

  def isDistinctFrom(rhs: T)(using pf: PgTypeFor[T]): Where[A] =
    isDistinctFrom(Param.bind(rhs)).asInstanceOf[Where[A]]

  def isNotDistinctFrom[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS NOT DISTINCT FROM ", rhs)

  def isNotDistinctFrom(rhs: T)(using pf: PgTypeFor[T]): Where[A] =
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

  given subqueryIn[T, Q, A](using ev: skunk.sharp.dsl.AsSubquery[Q, T, A]): InRhs[T, Q] =
    new InRhs[T, Q] {
      def renderParens(q: Q): Fragment[Void] = {
        val inner: Fragment[A] = ev.fragment(q)
        // Bind the inner Args at Void — typed-args threading through `IN (subquery)` is roadmap.
        val voidFrag = TypedExpr.liftAfToVoid(inner.apply(Void.asInstanceOf[A]))
        TypedExpr.wrap("(", voidFrag, ")")
      }
    }

}

extension [T, A](lhs: TypedExpr[T, A]) {

  /** `lhs IN (...)`. RHS bound parameters are baked into the encoder; result Args = LHS Args. */
  def in[Rhs](rhs: Rhs)(using ev: InRhs[T, Rhs]): Where[A] = {
    val rhsFrag = ev.renderParens(rhs)
    val combined = TypedExpr.combineSep(lhs.fragment, " IN ", rhsFrag)
    Where(combined.asInstanceOf[Fragment[A]])
  }

}

/**
 * ANY / ALL quantifier over a subquery RHS. Renders as `<lhs> <op> ANY (<subquery>)` / `<lhs> <op> ALL
 * (<subquery>)`. The inner subquery's args are baked via contramap so result Args = LHS Args.
 */
private def quantifiedRender[T, A, Q, ET, QA](
  lhs: TypedExpr[T, A],
  op: String,
  quant: String,
  q: Q
)(using
  ev: skunk.sharp.dsl.AsSubquery[Q, ET, QA],
  c2: Where.Concat2[A, Void]
): Where[A] = {
  // The inner subquery's encoder may carry baked Param values (e.g. inner WHERE has `like(...)`). Apply to
  // Void to bind any inner args, lift back to Fragment[Void] preserving the contramap-Void encoder, then
  // wrap with `<op> ANY/ALL (...)` and combineSep with the outer LHS so both encoders fold into the result.
  val inner: Fragment[QA] = ev.fragment(q)
  val voidFrag            = TypedExpr.liftAfToVoid(inner.apply(Void.asInstanceOf[QA]))
  val wrapped             = TypedExpr.wrap(s"$op $quant (", voidFrag, ")")
  val combined            = TypedExpr.combineSep[A, Void](lhs.fragment, " ", wrapped)
  Where(combined.asInstanceOf[Fragment[A]])
}

extension [T, A](lhs: TypedExpr[T, A])(using @unused ord: cats.Order[T]) {

  def ltAny[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A]  = quantifiedRender(lhs, "<",  "ANY", q)
  def lteAny[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A] = quantifiedRender(lhs, "<=", "ANY", q)
  def gtAny[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A]  = quantifiedRender(lhs, ">",  "ANY", q)
  def gteAny[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A] = quantifiedRender(lhs, ">=", "ANY", q)

  def ltAll[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A]  = quantifiedRender(lhs, "<",  "ALL", q)
  def lteAll[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A] = quantifiedRender(lhs, "<=", "ALL", q)
  def gtAll[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A]  = quantifiedRender(lhs, ">",  "ALL", q)
  def gteAll[Q, QA](q: Q)(using skunk.sharp.dsl.AsSubquery[Q, T, QA]): Where[A] = quantifiedRender(lhs, ">=", "ALL", q)

}
