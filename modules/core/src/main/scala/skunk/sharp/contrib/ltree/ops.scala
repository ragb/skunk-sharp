package skunk.sharp.contrib.ltree

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * `ltree` operator surface.
 *
 *   - `.matches(lquery)` — `path ~ pattern` (boolean).
 *   - `.matchesTxt(ltxtquery)` — `path @ query` (boolean).
 *   - `.isAncestorOf(other)` — `a @> b` (boolean).
 *   - `.isDescendantOf(other)` — `a <@ b` (boolean).
 *   - `.concat(other)` — `a || b` returning `ltree`.
 *
 * Plain English names instead of symbols (`%`, `<->`, …) keep operator overload search well-behaved and align with the
 * core DSL's convention.
 */
extension [A](lhs: TypedExpr[LTree, A]) {

  /** `lhs ~ pattern` — does the path match the `lquery` pattern? */
  inline def matches[B](pattern: TypedExpr[LQuery, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[LTree, LQuery, Boolean, A, B]("~")(lhs, pattern)
    Where(expr.fragment)
  }

  /** `lhs @ query` — does the path match the `ltxtquery`? */
  inline def matchesTxt[B](pattern: TypedExpr[LTxtQuery, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[LTree, LTxtQuery, Boolean, A, B]("@")(lhs, pattern)
    Where(expr.fragment)
  }

  /** `lhs @> rhs` — is `lhs` an ancestor of `rhs` (or equal)? */
  inline def isAncestorOf[B](rhs: TypedExpr[LTree, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[LTree, LTree, Boolean, A, B]("@>")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs <@ rhs` — is `lhs` a descendant of `rhs` (or equal)? */
  inline def isDescendantOf[B](rhs: TypedExpr[LTree, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[LTree, LTree, Boolean, A, B]("<@")(lhs, rhs)
    Where(expr.fragment)
  }

  /** `lhs || rhs` — append paths. Returns `ltree`. */
  inline def concat[B](rhs: TypedExpr[LTree, B]): TypedExpr[LTree, Where.Concat[A, B]] =
    PgOperator.infix[LTree, LTree, LTree, A, B]("||")(lhs, rhs)

}
