package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec}
import skunk.sharp.TypedExpr

/**
 * Row-compatible combination of two or more queries via `UNION` / `INTERSECT` / `EXCEPT` (and their `ALL` variants).
 *
 * Held **lazily**: each chained step appends to a render thunk that is only invoked at the single terminal `.compile`
 * call. No `AppliedFragment` is materialised until then — intermediate `.union`, `.intersect`, `.except` calls just
 * extend a closure chain. The codec is carried through eagerly (it's cheap — no SQL involved) and reused for the
 * combined result, since row-compatibility of set operations requires identical output types on every arm.
 *
 * Constructed through the [[union]] / [[intersect]] / [[except]] extensions on a [[SelectBuilder]],
 * [[ProjectedSelect]], [[CompiledQuery]], or another [[SetOpQuery]]. All sides flow through [[AsSubquery]], so users
 * never need to call `.compile` on the individual arms.
 *
 * {{{
 *   val active   = users.select.where(u => u.deleted_at.isNull)
 *   val inactive = users.select.where(u => u.deleted_at.isNotNull)
 *   active.union(inactive).compile        // → CompiledQuery[NamedRow[...]]
 * }}}
 *
 * Parenthesisation: each arm is wrapped in `(...)` in the emitted SQL so its own WHERE / ORDER BY / LIMIT don't bleed
 * into the set-op scope.
 */
final class SetOpQuery[R] private[sharp] (
  val codec: Codec[R],
  private[sharp] val renderFn: () => AppliedFragment
) {

  /** Materialise the entire chain into a terminal [[CompiledQuery]] — the single walking point for the tree. */
  def compile: CompiledQuery[R] = CompiledQuery(renderFn(), codec)

  /** `(<this>) UNION (<right>)` — deduplicated union. */
  def union[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("UNION", right)

  /** `(<this>) UNION ALL (<right>)` — keeps duplicates. */
  def unionAll[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("UNION ALL", right)

  /** `(<this>) INTERSECT (<right>)` — deduplicated intersection. */
  def intersect[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("INTERSECT", right)

  /** `(<this>) INTERSECT ALL (<right>)` — multiset intersection. */
  def intersectAll[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("INTERSECT ALL", right)

  /** `(<this>) EXCEPT (<right>)` — deduplicated difference. */
  def except[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("EXCEPT", right)

  /** `(<this>) EXCEPT ALL (<right>)` — multiset difference. */
  def exceptAll[Q](right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = append("EXCEPT ALL", right)

  /**
   * Append one set-op arm to the chain. Captures `ev` and `right` by closure — no compilation happens here; the thunk
   * only runs when `.compile` is ultimately called on the head of the chain.
   */
  private def append[Q](op: String, right: Q)(using ev: AsSubquery[Q, R]): SetOpQuery[R] = {
    val leftFn  = renderFn
    val rightFn = ev.render(right)
    new SetOpQuery[R](codec, () => leftFn() |+| TypedExpr.raw(s" $op (") |+| rightFn() |+| TypedExpr.raw(")"))
  }

}

object SetOpQuery {

  /**
   * Seed a chain: `(<arm>)`. Only called by the start-of-chain extensions on [[SelectBuilder]], [[ProjectedSelect]],
   * and [[CompiledQuery]]. The left thunk is wrapped in parens up front so later appends can just glue ` OP (<right>)`
   * on the tail.
   */
  private[sharp] def start[T, Q](q: Q)(using ev: AsSubquery[Q, T]): SetOpQuery[T] = {
    val inner = ev.render(q)
    new SetOpQuery[T](ev.codec(q), () => TypedExpr.raw("(") |+| inner() |+| TypedExpr.raw(")"))
  }

}

/**
 * Start a set-op chain from any [[AsSubquery]]-shaped thing that carries a concrete codec type `T`. Bringing this on as
 * an extension keeps the entry points consistent — users write `left.union(right)` regardless of whether `left` is a
 * [[SelectBuilder]], [[ProjectedSelect]], [[CompiledQuery]], or another [[SetOpQuery]].
 *
 * The `T` is inferred from the `AsSubquery` instance on the left; the right-hand side must then resolve to the same `T`
 * via its own `AsSubquery` — row-compatibility enforced statically.
 */
extension [Q](left: Q) {

  /** `(<left>) UNION (<right>)` — deduplicated. */
  def union[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).union(right)

  /** `(<left>) UNION ALL (<right>)` — keeps duplicates. */
  def unionAll[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).unionAll(right)

  /** `(<left>) INTERSECT (<right>)` — deduplicated intersection. */
  def intersect[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).intersect(right)

  /** `(<left>) INTERSECT ALL (<right>)` — multiset intersection. */
  def intersectAll[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).intersectAll(right)

  /** `(<left>) EXCEPT (<right>)` — deduplicated difference. */
  def except[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).except(right)

  /** `(<left>) EXCEPT ALL (<right>)` — multiset difference. */
  def exceptAll[T, Q2](right: Q2)(using evL: AsSubquery[Q, T], evR: AsSubquery[Q2, T]): SetOpQuery[T] =
    SetOpQuery.start[T, Q](left).exceptAll(right)

}
