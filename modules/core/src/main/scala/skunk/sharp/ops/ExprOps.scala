package skunk.sharp.ops

import skunk.{Fragment, Void}
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where
import skunk.util.Origin

import scala.annotation.unused

/**
 * The expression-level operator set: `=, <>, <, <=, >, >=, BETWEEN, IN, LIKE, IS NULL`. Each operator produces a
 * `Where[A]` (= `TypedExpr[Boolean, A]`) — a typed predicate carrying its parameter tuple as a visible Args type.
 * Operators slot wherever a boolean expression is valid in Postgres: WHERE, HAVING, SELECT projections, ORDER BY,
 * function arguments, CASE WHEN predicates.
 *
 * Operators are *extension methods* on `TypedExpr[T, A]` so third-party modules add new ones without touching core.
 *
 * **RHS forms** for binary operators — RHS is always a `TypedExpr`. To compare against a value pick one of:
 *
 *   - `lhs === Param[T]` — deferred parameter, supplied at execute time. Args contributes `T`. The static-SQL path: one
 *     `Fragment[T]` is built and reused across every argument value.
 *   - `lhs === lit(v)` — compile-time literal (primitives only). Inline SQL, Args contributes `Void`.
 *   - `lhs === otherExpr` — column-vs-expression / function-call result. Args from `otherExpr`.
 *   - `lhs === Param.bind(v)` — bake a runtime value into a `Void`-args fragment now. Rebuilds an encoder closure per
 *     `.compile`; pick this when the value really can't be deferred.
 *
 * **Nullable columns.** If a column is declared nullable, comparisons like `col === Param[T]` take the underlying value
 * type, not `Option[value]`. Trying to compare against `None` is a compile error — use `.isNull` / `.isNotNull`
 * instead. See [[Stripped]].
 */

/**
 * Type-level alias: strip outermost `Option[_]` if there is one, otherwise unchanged. Used as an evidence bound in
 * `like` / `ilike` / `similarTo` / `notSimilarTo` so a nullable-string column (`TypedColumn[Option[String], true, _]`)
 * accepts those operators — `Stripped[Option[String]] <:< String` resolves cleanly. Also used by
 * [[skunk.sharp.pg.functions.Shared.StrLike]] and [[skunk.sharp.pg.functions.PgSrf]]'s `nullif`.
 */
type Stripped[T] = T match {
  case Option[x] => x
  case _         => T
}

/** Build a `Where[Concat[A, B]]` from `lhs <op> rhs`. Both arms are typed expressions; Args from each propagate. */
private inline def opCombine[T, U, A, B](
  lhs: TypedExpr[T, A],
  opSql: String,
  rhs: TypedExpr[U, B]
): Where[Where.Concat[A, B]] = {
  val frag = TypedExpr.combineSepInl[A, B](lhs.fragment, opSql, rhs.fragment)
  Where(frag)
}

// Each operator's typed-RHS form and value-RHS form live in *separate* extension blocks. Putting them in a
// single block confuses Scala 3's overload resolution when one branch has a `using` clause and the match-type
// `T` appears in a parameter position — overload search fails before the `using` is summoned.

extension [T, A](lhs: TypedExpr[T, A]) {

  /** `lhs = rhs` — RHS is any TypedExpr. Use `Param[T]`, `lit(v)`, or `Param.bind(v)` for value RHS. */
  inline def ===[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)

  inline def !==[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " <> ", rhs)

  inline def <[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " < ", rhs)

  inline def <=[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " <= ", rhs)

  inline def >[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " > ", rhs)

  inline def >=[B](rhs: TypedExpr[T, B])(using @unused ord: cats.Order[T]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " >= ", rhs)

}

/** Column-to-expression equality alias for source compat. Equivalent to `===` with TypedExpr RHS. */
extension [T, A](lhs: TypedExpr[T, A]) {

  /** Same as `===` with TypedExpr RHS — column-vs-column / column-vs-function-call. */
  inline def ====[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] = opCombine(lhs, " = ", rhs)

}

/** `lhs BETWEEN lo AND hi` family. RHS bounds must be `TypedExpr`s — pass `Param[T]`, `lit(v)`, or `Param.bind(v)`. */
extension [T, A](lhs: TypedExpr[T, A]) {

  inline def between[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSepInl[B, C](lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

  inline def notBetween[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSepInl[B, C](lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " NOT BETWEEN ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

  inline def betweenSymmetric[B, C](lo: TypedExpr[T, B], hi: TypedExpr[T, C])(using
    @unused ord: cats.Order[T]
  ): Where[Where.Concat[A, Where.Concat[B, C]]] = {
    val rhs = TypedExpr.combineSepInl[B, C](lo.fragment, " AND ", hi.fragment)
    opCombine(lhs, " BETWEEN SYMMETRIC ", TypedExpr[T, Where.Concat[B, C]](rhs, lo.codec))
  }

}

/** `lhs IS DISTINCT FROM rhs` / `lhs IS NOT DISTINCT FROM rhs` — NULL-safe (in)equality. */
extension [T, A](lhs: TypedExpr[T, A]) {

  inline def isDistinctFrom[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS DISTINCT FROM ", rhs)

  inline def isNotDistinctFrom[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS NOT DISTINCT FROM ", rhs)

  /** Source-compat aliases for the column-vs-column NULL-safe variants. */
  inline def isDistinctFromExpr[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS DISTINCT FROM ", rhs)

  inline def isNotDistinctFromExpr[B](rhs: TypedExpr[T, B]): Where[Where.Concat[A, B]] =
    opCombine(lhs, " IS NOT DISTINCT FROM ", rhs)

}

/**
 * `lhs LIKE pattern` / `ILIKE` / `SIMILAR TO`. Pattern must be a `TypedExpr[String, _]` — use `lit("…%")` or
 * `Param[String]`.
 */
extension [T, A](lhs: TypedExpr[T, A]) {

  inline def like[B](pattern: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] =
    opCombine(lhs, " LIKE ", pattern)

  inline def ilike[B](pattern: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] =
    opCombine(lhs, " ILIKE ", pattern)

  inline def similarTo[B](pattern: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] =
    opCombine(lhs, " SIMILAR TO ", pattern)

  inline def notSimilarTo[B](pattern: TypedExpr[String, B])(using
    @unused ev: Stripped[T] <:< String
  ): Where[Where.Concat[A, B]] =
    opCombine(lhs, " NOT SIMILAR TO ", pattern)

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
 * `lhs IN (values...)` / `lhs IN (subquery)`. The RHS evidence builds a typed parenthesised fragment whose `Args`
 * surface as the `RA` slot; the outer `.in` extension threads them via `Concat[A, RA]`.
 */
sealed trait InRhs[T, Rhs] {
  type RA
  def renderParens(rhs: Rhs): Fragment[RA]
}

object InRhs {

  type Aux[T, Rhs, A0] = InRhs[T, Rhs] { type RA = A0 }

  /**
   * `lhs IN (e1, e2, …)` over a non-empty `Reducible` of typed expressions. Each item must be a `TypedExpr[T, Void]` —
   * pass `lit(v)` (compile-time literal), `Param.bind(v)` (bake runtime value), or any other Void-args expression. For
   * execute-time-deferred lists, prefer `lhs === ANY(Param[Arr[T]])` (see [[skunk.sharp.pg.ArrayOps.elemOf]]) — `IN`
   * with multiple `Param[T]` would require N execute-time slots which the API doesn't model.
   */
  given reducibleIn[T, F[_]](using R: cats.Reducible[F]): InRhs.Aux[T, F[TypedExpr[T, Void]], Void] =
    new InRhs[T, F[TypedExpr[T, Void]]] {
      type RA = Void
      def renderParens(values: F[TypedExpr[T, Void]]): Fragment[Void] = {
        val frags  = R.toNonEmptyList(values).toList.map(_.fragment)
        val joined = frags.reduceLeft((a, b) =>
          TypedExpr.combineSepInl[Void, Void](a, ", ", b).asInstanceOf[Fragment[Void]]
        )
        TypedExpr.wrap("(", joined, ")")
      }
    }

  given subqueryIn[T, Q, A](using ev: skunk.sharp.dsl.AsSubquery[Q, T, A]): InRhs.Aux[T, Q, A] =
    new InRhs[T, Q] {
      type RA = A
      def renderParens(q: Q): Fragment[A] = {
        val inner: Fragment[A] = ev.fragment(q)
        TypedExpr.wrap("(", inner, ")")
      }
    }

  /**
   * `lhs IN (listExpr)` where `listExpr` is a `Param[List[T]]` (built via [[skunk.sharp.Param.list]]) — expands to N
   * comma-separated `$N` placeholders and binds a single `List[T]` at execute time. Single prepared statement per size;
   * sidesteps the `Param.bind`-per-element pattern.
   */
  given paramListIn[T]: InRhs.Aux[T, skunk.sharp.Param[List[T]], List[T]] =
    new InRhs[T, skunk.sharp.Param[List[T]]] {
      type RA = List[T]
      def renderParens(p: skunk.sharp.Param[List[T]]): Fragment[List[T]] =
        TypedExpr.wrap("(", p.fragment, ")")
    }

}

extension [T, A](lhs: TypedExpr[T, A]) {

  /** `lhs IN (...)`. Param-bearing inner subqueries thread their `Args` into the result via `Concat[A, RA]`. */
  inline def in[Rhs, RA](rhs: Rhs)(using
    ev: InRhs.Aux[T, Rhs, RA]
  ): Where[Where.Concat[A, RA]] = {
    val rhsFrag  = ev.renderParens(rhs)
    val combined = TypedExpr.combineSepInl[A, RA](lhs.fragment, " IN ", rhsFrag)
    Where(combined)
  }

}

/**
 * ANY / ALL quantifier over a subquery RHS. Renders as `<lhs> <op> ANY (<subquery>)` / `<lhs> <op> ALL (<subquery>)`.
 * Param-bearing inner subqueries thread their `QA` slot into the result via `Concat[A, QA]`.
 */
private inline def quantifiedRender[T, A, Q, ET, QA](
  lhs: TypedExpr[T, A],
  op: String,
  quant: String,
  q: Q
)(using
  ev: skunk.sharp.dsl.AsSubquery[Q, ET, QA]
): Where[Where.Concat[A, QA]] = {
  val inner    = ev.fragment(q)
  val wrapped  = TypedExpr.wrap(s"$op $quant (", inner, ")")
  val combined = TypedExpr.combineSepInl[A, QA](lhs.fragment, " ", wrapped)
  Where(combined)
}

extension [T, A](lhs: TypedExpr[T, A])(using @unused ord: cats.Order[T]) {

  inline def ltAny[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, "<", "ANY", q)

  inline def lteAny[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, "<=", "ANY", q)

  inline def gtAny[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, ">", "ANY", q)

  inline def gteAny[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, ">=", "ANY", q)

  inline def ltAll[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, "<", "ALL", q)

  inline def lteAll[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, "<=", "ALL", q)

  inline def gtAll[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, ">", "ALL", q)

  inline def gteAll[Q, QA](q: Q)(using
    skunk.sharp.dsl.AsSubquery[Q, T, QA]
  ): Where[Where.Concat[A, QA]] = quantifiedRender(lhs, ">=", "ALL", q)

}
