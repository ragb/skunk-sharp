package skunk.sharp.pg

import skunk.sharp.TypedExpr
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

/** Range operators as extension methods on `TypedExpr[R, X]` where `IsRange[R]` holds. Args from both arms propagate. */
object RangeOps {

  private def boolOp[R, X, Y](op: String, l: TypedExpr[R, X], r: TypedExpr[R, Y])(using
    c2: Where.Concat2[X, Y]
  ): Where[Where.Concat[X, Y]] = {
    val frag = TypedExpr.combineSep(l.fragment, s" $op ", r.fragment)
    Where(frag)
  }

  private def rangeOp[R, X, Y](op: String, l: TypedExpr[R, X], r: TypedExpr[R, Y])(using
    c2: Where.Concat2[X, Y]
  ): TypedExpr[R, Where.Concat[X, Y]] = {
    val frag = TypedExpr.combineSep(l.fragment, s" $op ", r.fragment)
    TypedExpr[R, Where.Concat[X, Y]](frag, l.codec)
  }

  extension [R, X](lhs: TypedExpr[R, X])(using @annotation.unused ev: IsRange[R]) {

    def contains[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):           Where[Where.Concat[X, Y]] = boolOp("@>",  lhs, rhs)
    def containedBy[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):        Where[Where.Concat[X, Y]] = boolOp("<@",  lhs, rhs)
    def overlaps[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):           Where[Where.Concat[X, Y]] = boolOp("&&",  lhs, rhs)
    def strictlyLeft[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):       Where[Where.Concat[X, Y]] = boolOp("<<",  lhs, rhs)
    def strictlyRight[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):      Where[Where.Concat[X, Y]] = boolOp(">>",  lhs, rhs)
    def doesNotExtendRight[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]): Where[Where.Concat[X, Y]] = boolOp("&<",  lhs, rhs)
    def doesNotExtendLeft[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):  Where[Where.Concat[X, Y]] = boolOp("&>",  lhs, rhs)
    def adjacent[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):           Where[Where.Concat[X, Y]] = boolOp("-|-", lhs, rhs)

    def rangeUnion[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):     TypedExpr[R, Where.Concat[X, Y]] = rangeOp("+", lhs, rhs)
    def rangeIntersect[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]): TypedExpr[R, Where.Concat[X, Y]] = rangeOp("*", lhs, rhs)
    def rangeDiff[Y](rhs: TypedExpr[R, Y])(using Where.Concat2[X, Y]):      TypedExpr[R, Where.Concat[X, Y]] = rangeOp("-", lhs, rhs)

  }

  extension [R, E, X](lhs: TypedExpr[R, X])(using @annotation.unused ev: IsRange.Aux[R, E]) {

    /** `a @> e` — range contains the given element. */
    def containsElem[Y](elem: TypedExpr[E, Y])(using Where.Concat2[X, Y]): Where[Where.Concat[X, Y]] = {
      val frag = TypedExpr.combineSep(lhs.fragment, " @> ", elem.fragment)
      Where(frag)
    }

    /** `e <@ a` — element is contained in this range. */
    def elemContainedBy[Y](elem: TypedExpr[E, Y])(using Where.Concat2[Y, X]): Where[Where.Concat[Y, X]] = {
      val frag = TypedExpr.combineSep(elem.fragment, " <@ ", lhs.fragment)
      Where(frag)
    }

  }

}
