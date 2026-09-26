package skunk.sharp.contrib.pgvector

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * pgvector distance operators, named (like the rest of the DSL) rather than symbolic. Both sides must have the same
 * dimension `N` — a mismatch is a compile error. Each returns `double precision`, so it can be selected, filtered on,
 * or ordered by (`ORDER BY embedding <=> $1 LIMIT k` is the nearest-neighbour query an HNSW / IVFFlat index serves).
 *
 *   - `.cosineDistance(q)` — `<=>`; the usual choice for text embeddings.
 *   - `.l2Distance(q)` — `<->`, Euclidean.
 *   - `.negativeInnerProduct(q)` — `<#>`; for normalised vectors, ordering by it ascending = most similar first.
 *   - `.l1Distance(q)` — `<+>`, taxicab (pgvector 0.7+).
 */
extension [N <: Int, A](lhs: TypedExpr[PgVector[N], A]) {

  inline def cosineDistance[B](rhs: TypedExpr[PgVector[N], B]): TypedExpr[Double, Where.Concat[A, B]] =
    PgOperator.infix[PgVector[N], PgVector[N], Double, A, B]("<=>")(lhs, rhs)

  inline def l2Distance[B](rhs: TypedExpr[PgVector[N], B]): TypedExpr[Double, Where.Concat[A, B]] =
    PgOperator.infix[PgVector[N], PgVector[N], Double, A, B]("<->")(lhs, rhs)

  inline def negativeInnerProduct[B](rhs: TypedExpr[PgVector[N], B]): TypedExpr[Double, Where.Concat[A, B]] =
    PgOperator.infix[PgVector[N], PgVector[N], Double, A, B]("<#>")(lhs, rhs)

  inline def l1Distance[B](rhs: TypedExpr[PgVector[N], B]): TypedExpr[Double, Where.Concat[A, B]] =
    PgOperator.infix[PgVector[N], PgVector[N], Double, A, B]("<+>")(lhs, rhs)

}
