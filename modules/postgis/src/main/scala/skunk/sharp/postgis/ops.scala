package skunk.sharp.postgis

import skunk.postgis.*
import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * Operator surface on `TypedExpr[T <: Geometry, A]`. Method names spell out the operator (`.bboxOverlaps`,
 * `.bboxContains`) instead of taking the symbolic forms (`&&`, `~`, `@`); same convention as the other contribs.
 * `ST_*` predicates also have method aliases (`.distance`, `.contains`, …) for shorter call sites.
 */
extension [T <: Geometry, A](lhs: TypedExpr[T, A]) {

  private inline def lhsG: TypedExpr[Geometry, A] = PgPostgis.widen(lhs)

  // -------- Bounding-box operators (cheap, GiST-index-backed) ----------------------------------

  /** `lhs && rhs` — bounding boxes overlap. The standard PostGIS index probe; fast on a GiST index. */
  inline def bboxOverlaps[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Geometry, Geometry, Boolean, A, B]("&&")(lhsG, PgPostgis.widen(rhs))
    Where(expr.fragment)
  }

  /** `lhs ~ rhs` — `lhs`'s bbox contains `rhs`'s bbox. */
  inline def bboxContains[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Geometry, Geometry, Boolean, A, B]("~")(lhsG, PgPostgis.widen(rhs))
    Where(expr.fragment)
  }

  /** `lhs @ rhs` — `lhs`'s bbox is contained by `rhs`'s bbox. */
  inline def bboxWithin[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] = {
    val expr = PgOperator.infix[Geometry, Geometry, Boolean, A, B]("@")(lhsG, PgPostgis.widen(rhs))
    Where(expr.fragment)
  }

  // -------- ST_* predicate aliases — same as the PgPostgis function calls, in postfix form -----

  inline def distance[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): TypedExpr[Double, Where.Concat[A, B]] =
    PgPostgis.distance(lhs, rhs)

  inline def dWithin[T2 <: Geometry, B, C](
    rhs: TypedExpr[T2, B],
    radius: TypedExpr[Double, C]
  ): TypedExpr[Boolean, Where.Concat[A, Where.Concat[B, C]]] =
    PgPostgis.dWithin(lhs, rhs, radius)

  inline def contains[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.contains(lhs, rhs).fragment)

  inline def within[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.within(lhs, rhs).fragment)

  inline def intersects[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.intersects(lhs, rhs).fragment)

  inline def crosses[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.crosses(lhs, rhs).fragment)

  inline def overlapsSpatial[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.overlaps(lhs, rhs).fragment)

  inline def disjoint[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.disjoint(lhs, rhs).fragment)

  inline def stEquals[T2 <: Geometry, B](rhs: TypedExpr[T2, B]): Where[Where.Concat[A, B]] =
    Where(PgPostgis.stEquals(lhs, rhs).fragment)

}
