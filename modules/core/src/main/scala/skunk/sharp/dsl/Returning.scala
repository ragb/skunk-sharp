package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.TypedExpr
import skunk.sharp.internal.RowCodecs.tupleCodec

/** Shared bodies of the `returningTuple` / `returningNamed` forms, for every builder with RETURNING. */
private[dsl] object Returning {

  /**
   * A (named) tuple of RETURNING expressions as one comma-joined `TypedExpr`: its Args fold the items' Args (split back
   * by `pa`), its codec is the tuple of the items' codecs.
   */
  def tuple[A, R](items: Product, pa: ProjArgsOf[?]): TypedExpr[R, A] = {
    val exprs   = items.productIterator.toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val project = pa.project.asInstanceOf[A => List[Any]]
    TypedExpr[R, A](
      TypedExpr.combineList[A](exprs.map(_.fragment), ", ", project),
      tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[R]]
    )
  }

}
