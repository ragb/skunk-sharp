package skunk.sharp.pg.functions

import skunk.Codec
import skunk.sharp.TypedExpr

trait PgGrouping {

  val emptyGroup: Seq[TypedExpr[?]] = Seq.empty

  def groupingSets(sets: Seq[TypedExpr[?]]*): TypedExpr[Unit] = {
    val setFrags = sets.toList.map { set =>
      if (set.isEmpty) TypedExpr.raw("()")
      else TypedExpr.raw("(") |+| TypedExpr.joined(set.map(_.render).toList, ", ") |+| TypedExpr.raw(")")
    }
    TypedExpr(
      TypedExpr.raw("GROUPING SETS (") |+| TypedExpr.joined(setFrags, ", ") |+| TypedExpr.raw(")"),
      groupSpecCodec
    )
  }

  def rollup(exprs: TypedExpr[?]*): TypedExpr[Unit] = {
    val inner = TypedExpr.joined(exprs.toList.map(_.render), ", ")
    TypedExpr(TypedExpr.raw("ROLLUP (") |+| inner |+| TypedExpr.raw(")"), groupSpecCodec)
  }

  def cube(exprs: TypedExpr[?]*): TypedExpr[Unit] = {
    val inner = TypedExpr.joined(exprs.toList.map(_.render), ", ")
    TypedExpr(TypedExpr.raw("CUBE (") |+| inner |+| TypedExpr.raw(")"), groupSpecCodec)
  }

  def grouping(cols: TypedExpr[?]*): TypedExpr[Int] = {
    val inner = TypedExpr.joined(cols.toList.map(_.render), ", ")
    TypedExpr(TypedExpr.raw("GROUPING(") |+| inner |+| TypedExpr.raw(")"), skunk.codec.all.int4)
  }

  private val groupSpecCodec: Codec[Unit] =
    skunk.codec.all.bool.imap(_ => ())(_ => false)

}
