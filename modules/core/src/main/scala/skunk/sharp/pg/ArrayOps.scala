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

/** Array-only operators. Args from both arms propagate. */
object ArrayOps {

  // `contains` / `containedBy` / `overlaps` / `concat` on arrays are the shared, typeclass-dispatched operators in
  // `skunk.sharp.ops` (exported from `skunk.sharp.dsl`).

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
