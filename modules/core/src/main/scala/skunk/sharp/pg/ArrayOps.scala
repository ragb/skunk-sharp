package skunk.sharp.pg

import skunk.data.Arr
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Evidence that `A` is a Postgres-array-shaped Scala type.
 */
sealed trait IsArray[A] {
  type Elem
}

object IsArray {

  type Aux[A, E] = IsArray[A] { type Elem = E }

  given arrIsArray[T]: IsArray.Aux[Arr[T], T] = new IsArray[Arr[T]] { type Elem = T }

}

/** Array operators as extension methods on `TypedExpr[A, X]` where `IsArray[A]`. Args from both arms propagate. */
object ArrayOps {

  private inline def boolOp[A, X, Y](op: String, l: TypedExpr[A, X], r: TypedExpr[A, Y]): Where[Where.Concat[X, Y]] = {
    val frag = TypedExpr.combineSepInl[X, Y](l.fragment, s" $op ", r.fragment)
    Where(frag)
  }

  extension [A, X](lhs: TypedExpr[A, X])(using @annotation.unused ev: IsArray[A]) {

    inline def contains[Y](rhs: TypedExpr[A, Y]): Where[Where.Concat[X, Y]]    = boolOp("@>", lhs, rhs)
    inline def containedBy[Y](rhs: TypedExpr[A, Y]): Where[Where.Concat[X, Y]] = boolOp("<@", lhs, rhs)
    inline def overlaps[Y](rhs: TypedExpr[A, Y]): Where[Where.Concat[X, Y]]    = boolOp("&&", lhs, rhs)

    inline def concat[Y](rhs: TypedExpr[A, Y]): TypedExpr[A, Where.Concat[X, Y]] = {
      val frag = TypedExpr.combineSepInl[X, Y](lhs.fragment, " || ", rhs.fragment)
      TypedExpr[A, Where.Concat[X, Y]](frag, lhs.codec)
    }

  }

  extension [E, X](elem: TypedExpr[E, X]) {

    /** `elem = ANY(array)`. */
    inline def elemOf[A, Y](arr: TypedExpr[A, Y])(using
      @annotation.unused ev: IsArray.Aux[A, E]
    ): Where[Where.Concat[X, Y]] = {
      val arrWrapped = TypedExpr.wrap("ANY(", arr.fragment, ")")
      val frag       = TypedExpr.combineSepInl[X, Y](elem.fragment, " = ", arrWrapped)
      Where(frag)
    }

  }

}
