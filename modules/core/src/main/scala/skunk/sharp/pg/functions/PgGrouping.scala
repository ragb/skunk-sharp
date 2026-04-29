package skunk.sharp.pg.functions

import skunk.{Codec, Fragment, Void}
import skunk.sharp.TypedExpr

trait PgGrouping {

  val emptyGroup: Seq[TypedExpr[?, ?]] = Seq.empty

  def groupingSets(sets: Seq[TypedExpr[?, ?]]*): TypedExpr[Unit, Void] = {
    val setFrags: List[Fragment[Void]] = sets.toList.map { set =>
      if (set.isEmpty) TypedExpr.voidFragment("()")
      else {
        val joined = TypedExpr.joinedVoid(", ", set.toList.map(_.fragment))
        TypedExpr.wrap("(", joined, ")")
      }
    }
    val joined = TypedExpr.joinedVoid(", ", setFrags)
    val frag   = TypedExpr.wrap("GROUPING SETS (", joined, ")")
    TypedExpr[Unit, Void](frag, groupSpecCodec)
  }

  def rollup(exprs: TypedExpr[?, ?]*): TypedExpr[Unit, Void] = {
    val joined = TypedExpr.joinedVoid(", ", exprs.toList.map(_.fragment))
    val frag   = TypedExpr.wrap("ROLLUP (", joined, ")")
    TypedExpr[Unit, Void](frag, groupSpecCodec)
  }

  def cube(exprs: TypedExpr[?, ?]*): TypedExpr[Unit, Void] = {
    val joined = TypedExpr.joinedVoid(", ", exprs.toList.map(_.fragment))
    val frag   = TypedExpr.wrap("CUBE (", joined, ")")
    TypedExpr[Unit, Void](frag, groupSpecCodec)
  }

  def grouping(cols: TypedExpr[?, ?]*): TypedExpr[Int, Void] = {
    val joined = TypedExpr.joinedVoid(", ", cols.toList.map(_.fragment))
    val frag   = TypedExpr.wrap("GROUPING(", joined, ")")
    TypedExpr[Int, Void](frag, skunk.codec.all.int4)
  }

  private val groupSpecCodec: Codec[Unit] =
    skunk.codec.all.bool.imap(_ => ())(_ => false)

}
