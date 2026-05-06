package skunk.sharp.dsl

import skunk.{Codec, Fragment}
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Row-compatible combination of two or more queries via `UNION` / `INTERSECT` / `EXCEPT` (and their `ALL` variants).
 *
 * Each combinator step concatenates a typed `Fragment[A]` for the right arm via `combineSep`, so any `Param` in
 * either arm threads through to the outer query's `Args` via `Concat`. Held **lazily**: each chained step appends
 * to a render thunk that is only invoked at the single terminal `.compile` call.
 *
 * Constructed through the [[union]] / [[intersect]] / [[except]] extensions on a [[SelectBuilder]],
 * [[ProjectedSelect]], [[CompiledQuery]], or another [[SetOpQuery]]. All sides flow through [[AsSubquery]], so users
 * never need to call `.compile` on the individual arms.
 *
 * {{{
 *   val active   = users.select.where(u => u.deleted_at.isNull)
 *   val inactive = users.select.where(u => u.deleted_at.isNotNull)
 *   active.union(inactive).compile        // → CompiledQuery[Void, NamedRow[...]]
 * }}}
 *
 * Parenthesisation: each arm is wrapped in `(...)` in the emitted SQL so its own WHERE / ORDER BY / LIMIT don't bleed
 * into the set-op scope.
 */
final class SetOpQuery[A, R] @scala.annotation.publicInBinary private[sharp] (
  val codec: Codec[R],
  private[sharp] val renderFn: () => Fragment[A]
) {

  /** Materialise the entire chain into a terminal [[QueryTemplate]] — the single walking point for the tree. */
  def compile: QueryTemplate[A, R] = QueryTemplate.mk[A, R](renderFn(), codec)

  /** `(<this>) UNION (<right>)` — deduplicated union. */
  inline def union[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("UNION", right)

  /** `(<this>) UNION ALL (<right>)` — keeps duplicates. */
  inline def unionAll[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("UNION ALL", right)

  /** `(<this>) INTERSECT (<right>)` — deduplicated intersection. */
  inline def intersect[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("INTERSECT", right)

  /** `(<this>) INTERSECT ALL (<right>)` — multiset intersection. */
  inline def intersectAll[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("INTERSECT ALL", right)

  /** `(<this>) EXCEPT (<right>)` — deduplicated difference. */
  inline def except[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("EXCEPT", right)

  /** `(<this>) EXCEPT ALL (<right>)` — multiset difference. */
  inline def exceptAll[Q, B](right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = append("EXCEPT ALL", right)

  private inline def append[Q, B](op: String, right: Q)(using ev: AsSubquery[Q, R, B]): SetOpQuery[Where.Concat[A, B], R] = {
    val leftFn     = renderFn
    val rightFrag: Fragment[B] = ev.fragment(right)
    new SetOpQuery[Where.Concat[A, B], R](
      codec,
      () => {
        val opSep = TypedExpr.combineSepInl[A, B](leftFn(), s" $op (", rightFrag)
        TypedExpr.wrap("", opSep, ")")
      }
    )
  }

}

object SetOpQuery {

  /**
   * Seed a chain: `(<arm>)`. Only called by the start-of-chain extensions on [[SelectBuilder]], [[ProjectedSelect]],
   * and [[QueryTemplate]]. The left thunk is wrapped in parens up front so later appends can just glue
   * ` OP (<right>)` on the tail.
   */
  private[sharp] def start[T, Q, A](q: Q)(using ev: AsSubquery[Q, T, A]): SetOpQuery[A, T] = {
    val innerFrag: Fragment[A] = ev.fragment(q)
    new SetOpQuery[A, T](
      ev.codec(q),
      () => TypedExpr.wrap("(", innerFrag, ")")
    )
  }

}

/**
 * Start a set-op chain from any [[AsSubquery]]-shaped thing that carries a concrete codec type `T`. Bringing this on as
 * an extension keeps the entry points consistent — users write `left.union(right)` regardless of whether `left` is a
 * [[SelectBuilder]], [[ProjectedSelect]], [[CompiledQuery]], or another [[SetOpQuery]].
 *
 * The `T` is inferred from the `AsSubquery` instance on the left; the right-hand side must then resolve to the same `T`
 * via its own `AsSubquery` — row-compatibility enforced statically. Both sides' `Args` thread into the result via
 * `Concat[A1, A2]`.
 */
extension [Q](left: Q) {

  /** `(<left>) UNION (<right>)` — deduplicated. */
  inline def union[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).union(right)

  /** `(<left>) UNION ALL (<right>)` — keeps duplicates. */
  inline def unionAll[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).unionAll(right)

  /** `(<left>) INTERSECT (<right>)` — deduplicated intersection. */
  inline def intersect[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).intersect(right)

  /** `(<left>) INTERSECT ALL (<right>)` — multiset intersection. */
  inline def intersectAll[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).intersectAll(right)

  /** `(<left>) EXCEPT (<right>)` — deduplicated difference. */
  inline def except[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).except(right)

  /** `(<left>) EXCEPT ALL (<right>)` — multiset difference. */
  inline def exceptAll[T, Q2, A1, A2](right: Q2)(using evL: AsSubquery[Q, T, A1], evR: AsSubquery[Q2, T, A2]): SetOpQuery[Where.Concat[A1, A2], T] =
    SetOpQuery.start[T, Q, A1](left).exceptAll(right)

}
