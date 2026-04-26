package skunk.sharp.pg.functions

import skunk.{Codec, Fragment}
import skunk.sharp.TypedExpr

trait PgGrouping {

  val emptyGroup: Seq[TypedExpr[?, ?]] = Seq.empty

  def groupingSets(sets: Seq[TypedExpr[?, ?]]*): TypedExpr[Unit, ?] = {
    val setFrags: List[Fragment[?]] = sets.toList.map { set =>
      if (set.isEmpty) TypedExpr.voidFragment("()")
      else {
        val joined = joinFrags(set.map(_.fragment).toList, ", ")
        TypedExpr.wrap("(", joined, ")")
      }
    }
    val joined = joinFrags(setFrags, ", ")
    val frag   = TypedExpr.wrap("GROUPING SETS (", joined, ")").asInstanceOf[Fragment[Any]]
    TypedExpr[Unit, Any](frag, groupSpecCodec)
  }

  def rollup(exprs: TypedExpr[?, ?]*): TypedExpr[Unit, ?] = {
    val joined = joinFrags(exprs.toList.map(_.fragment), ", ")
    val frag   = TypedExpr.wrap("ROLLUP (", joined, ")").asInstanceOf[Fragment[Any]]
    TypedExpr[Unit, Any](frag, groupSpecCodec)
  }

  def cube(exprs: TypedExpr[?, ?]*): TypedExpr[Unit, ?] = {
    val joined = joinFrags(exprs.toList.map(_.fragment), ", ")
    val frag   = TypedExpr.wrap("CUBE (", joined, ")").asInstanceOf[Fragment[Any]]
    TypedExpr[Unit, Any](frag, groupSpecCodec)
  }

  def grouping(cols: TypedExpr[?, ?]*): TypedExpr[Int, ?] = {
    val joined = joinFrags(cols.toList.map(_.fragment), ", ")
    val frag   = TypedExpr.wrap("GROUPING(", joined, ")").asInstanceOf[Fragment[Any]]
    TypedExpr[Int, Any](frag, skunk.codec.all.int4)
  }

  private val groupSpecCodec: Codec[Unit] =
    skunk.codec.all.bool.imap(_ => ())(_ => false)

  private def joinFrags(parts: List[Fragment[?]], sep: String): Fragment[?] =
    parts match {
      case Nil          => TypedExpr.voidFragment("")
      case head :: Nil  => head
      case head :: tail =>
        tail.foldLeft(head.asInstanceOf[Fragment[Any]])((acc, p) =>
          TypedExpr.combineSep(acc, sep, p).asInstanceOf[Fragment[Any]]
        )
    }

}
