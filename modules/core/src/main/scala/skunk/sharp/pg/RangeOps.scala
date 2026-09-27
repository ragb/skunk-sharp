package skunk.sharp.pg

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * Evidence that `R` is a Postgres range-shaped Scala type.
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
 * Range operators as extension methods on `TypedExpr[R, X]` where `IsRange[R]` holds. Args from both arms propagate.
 */
object RangeOps {

  private inline def boolOp[R, X, Y](op: String, l: TypedExpr[R, X], r: TypedExpr[R, Y]): Where[Where.Concat[X, Y]] = {
    Where(PgOperator.binary[X, Y](op, l.fragment, r.fragment))
  }

  private inline def rangeOp[R, X, Y](
    op: String,
    l: TypedExpr[R, X],
    r: TypedExpr[R, Y]
  ): TypedExpr[R, Where.Concat[X, Y]] = {
    TypedExpr[R, Where.Concat[X, Y]](PgOperator.binary[X, Y](op, l.fragment, r.fragment), l.codec)
  }

  // `contains` / `containedBy` / `overlaps` (range-range and range-element) are the shared, typeclass-dispatched
  // operators in `skunk.sharp.ops` (exported from `skunk.sharp.dsl`).
  extension [R, X](lhs: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]) {

    inline def strictlyLeft[Y](rhs: TypedExpr[R, Y]): Where[Where.Concat[X, Y]]       = boolOp("<<", lhs, rhs)
    inline def strictlyRight[Y](rhs: TypedExpr[R, Y]): Where[Where.Concat[X, Y]]      = boolOp(">>", lhs, rhs)
    inline def doesNotExtendRight[Y](rhs: TypedExpr[R, Y]): Where[Where.Concat[X, Y]] = boolOp("&<", lhs, rhs)
    inline def doesNotExtendLeft[Y](rhs: TypedExpr[R, Y]): Where[Where.Concat[X, Y]]  = boolOp("&>", lhs, rhs)
    inline def adjacent[Y](rhs: TypedExpr[R, Y]): Where[Where.Concat[X, Y]]           = boolOp("-|-", lhs, rhs)

    inline def rangeUnion[Y](rhs: TypedExpr[R, Y]): TypedExpr[R, Where.Concat[X, Y]]     = rangeOp("+", lhs, rhs)
    inline def rangeIntersect[Y](rhs: TypedExpr[R, Y]): TypedExpr[R, Where.Concat[X, Y]] = rangeOp("*", lhs, rhs)
    inline def rangeDiff[Y](rhs: TypedExpr[R, Y]): TypedExpr[R, Where.Concat[X, Y]]      = rangeOp("-", lhs, rhs)

  }

  extension [R, E, X](lhs: TypedExpr[R, X])(using @annotation.unused ev: IsRange.Aux[R, E]) {

    /** `a @> e` — range contains the given element. */
    inline def containsElem[Y](elem: TypedExpr[E, Y]): Where[Where.Concat[X, Y]] = {
      Where(PgOperator.binary[X, Y]("@>", lhs.fragment, elem.fragment))
    }

    /** `e <@ a` — element is contained in this range. */
    inline def elemContainedBy[Y](elem: TypedExpr[E, Y]): Where[Where.Concat[Y, X]] = {
      Where(PgOperator.binary[Y, X]("<@", elem.fragment, lhs.fragment))
    }

  }

}
