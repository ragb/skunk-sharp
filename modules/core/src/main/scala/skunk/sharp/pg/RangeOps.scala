package skunk.sharp.pg

import skunk.codec.all as pg
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Evidence that `R` is a Postgres range-shaped Scala type. Carries the element type `Elem` so that `containsElem` /
 * `rangeContainedByElem` can mention the element without an extra type parameter at the call site.
 */
sealed trait IsRange[R] {
  type Elem
}

object IsRange {

  type Aux[R, E] = IsRange[R] { type Elem = E }

  given [E](using @annotation.unused pf: PgTypeFor[tags.PgRange[E]]): IsRange.Aux[tags.PgRange[E], E] =
    new IsRange[tags.PgRange[E]] { type Elem = E }

}

/**
 * Range operators as extension methods on `TypedExpr[R]` where `IsRange[R]` holds.
 *
 * Operator mapping (mirrors Postgres):
 *   - `.contains(r)` → `a @> b` — left range contains every point of right range
 *   - `.containsElem(e)` → `a @> e` — left range contains element
 *   - `.containedBy(r)` → `a <@ b`
 *   - `.overlaps(r)` → `a && b`
 *   - `.strictlyLeft(r)` → `a << b` — every point in `a` is < every point in `b`
 *   - `.strictlyRight(r)` → `a >> b`
 *   - `.doesNotExtendRight(r)` → `a &< b` — `a` does not extend to the right of `b`
 *   - `.doesNotExtendLeft(r)` → `a &> b`
 *   - `.adjacent(r)` → `a -|- b`
 *   - `.rangeUnion(r)` → `a + b` — union (must be contiguous or overlapping)
 *   - `.rangeIntersect(r)` → `a * b`
 *   - `.rangeDiff(r)` → `a - b` — difference
 */
object RangeOps {

  private def boolOp[R](op: String, l: TypedExpr[R], r: TypedExpr[R]): Where =
    Where(new TypedExpr[Boolean] {
      val render = l.render |+| TypedExpr.raw(s" $op ") |+| r.render
      val codec  = pg.bool
    })

  private def rangeOp[R](op: String, l: TypedExpr[R], r: TypedExpr[R]): TypedExpr[R] =
    TypedExpr(l.render |+| TypedExpr.raw(s" $op ") |+| r.render, l.codec)

  extension [R](lhs: TypedExpr[R])(using @annotation.unused ev: IsRange[R]) {

    /** `a @> b` — left range contains every point in right range. */
    def contains(rhs: TypedExpr[R]): Where = boolOp("@>", lhs, rhs)

    /** `a <@ b` — left range is contained by right range. */
    def containedBy(rhs: TypedExpr[R]): Where = boolOp("<@", lhs, rhs)

    /** `a && b` — ranges overlap (share at least one point). */
    def overlaps(rhs: TypedExpr[R]): Where = boolOp("&&", lhs, rhs)

    /** `a << b` — every point in left range is strictly less than every point in right range. */
    def strictlyLeft(rhs: TypedExpr[R]): Where = boolOp("<<", lhs, rhs)

    /** `a >> b` — every point in left range is strictly greater than every point in right range. */
    def strictlyRight(rhs: TypedExpr[R]): Where = boolOp(">>", lhs, rhs)

    /** `a &< b` — left range does not extend to the right of right range. */
    def doesNotExtendRight(rhs: TypedExpr[R]): Where = boolOp("&<", lhs, rhs)

    /** `a &> b` — left range does not extend to the left of right range. */
    def doesNotExtendLeft(rhs: TypedExpr[R]): Where = boolOp("&>", lhs, rhs)

    /** `a -|- b` — ranges are adjacent (no gap, no overlap). */
    def adjacent(rhs: TypedExpr[R]): Where = boolOp("-|-", lhs, rhs)

    /** `a + b` — range union (ranges must overlap or be adjacent). */
    def rangeUnion(rhs: TypedExpr[R]): TypedExpr[R] = rangeOp("+", lhs, rhs)

    /** `a * b` — range intersection. */
    def rangeIntersect(rhs: TypedExpr[R]): TypedExpr[R] = rangeOp("*", lhs, rhs)

    /** `a - b` — range difference. */
    def rangeDiff(rhs: TypedExpr[R]): TypedExpr[R] = rangeOp("-", lhs, rhs)

  }

  extension [R, E](lhs: TypedExpr[R])(using @annotation.unused ev: IsRange.Aux[R, E]) {

    /** `a @> e` — range contains the given element. */
    def containsElem(elem: TypedExpr[E]): Where =
      Where(new TypedExpr[Boolean] {
        val render = lhs.render |+| TypedExpr.raw(" @> ") |+| elem.render
        val codec  = pg.bool
      })

    /** `e <@ a` — element is contained in this range. Sugar for `range.containsElem(elem)` read from the elem side. */
    def elemContainedBy(elem: TypedExpr[E]): Where =
      Where(new TypedExpr[Boolean] {
        val render = elem.render |+| TypedExpr.raw(" <@ ") |+| lhs.render
        val codec  = pg.bool
      })

  }

}
