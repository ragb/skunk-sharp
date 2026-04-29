package skunk.sharp.pg.functions

import skunk.Fragment
import skunk.codec.all as pg
import skunk.data.Arr
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.pg.{IsArray, PgTypeFor}
import skunk.sharp.where.Where

/**
 * Built-in Postgres array functions. Operators (`@>`, `<@`, `&&`, `||`, `= ANY`) live as extensions in
 * [[skunk.sharp.pg.ArrayOps]]. Args of input expression(s) propagate to result.
 */
trait PgArray {

  def arrayLength[A, X](a: TypedExpr[A, X], dim: Int = 1)(using @annotation.unused ev: IsArray[A]): TypedExpr[Option[Int], X] = {
    val parts = a.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s", $dim)"))
    val frag  = Fragment[X](
      List[Either[String, cats.data.State[Int, String]]](Left("array_length(")) ++ parts,
      a.fragment.encoder, skunk.util.Origin.unknown
    )
    TypedExpr[Option[Int], X](frag, pg.int4.opt)
  }

  def cardinality[A, X](a: TypedExpr[A, X])(using @annotation.unused ev: IsArray[A]): TypedExpr[Int, X] = {
    val frag = TypedExpr.wrap("cardinality(", a.fragment, ")")
    TypedExpr[Int, X](frag, pg.int4)
  }

  def arrayAppend[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment)
    val frag  = TypedExpr.wrap("array_append(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayPrepend[A, E, X, Y](elem: TypedExpr[E, X], a: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(elem.fragment, ", ", a.fragment)
    val frag  = TypedExpr.wrap("array_prepend(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayCat[A, X, Y](a: TypedExpr[A, X], b: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray[A]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("array_cat(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayPosition[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Option[Int], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment)
    val frag  = TypedExpr.wrap("array_position(", inner, ")")
    TypedExpr[Option[Int], Where.Concat[X, Y]](frag, pg.int4.opt)
  }

  def arrayPositions[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Arr[Int], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment)
    val frag  = TypedExpr.wrap("array_positions(", inner, ")")
    TypedExpr[Arr[Int], Where.Concat[X, Y]](frag, pg._int4)
  }

  def arrayRemove[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment)
    val frag  = TypedExpr.wrap("array_remove(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayReplace[A, E](a: TypedExpr[A, ?], from: TypedExpr[E, ?], to: TypedExpr[E, ?])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, skunk.Void] = {
    val joined = TypedExpr.joinedVoid(", ", List(a.fragment, from.fragment, to.fragment))
    val frag   = TypedExpr.wrap("array_replace(", joined, ")")
    TypedExpr[A, skunk.Void](frag, a.codec)
  }

  def arrayToString[A, X](a: TypedExpr[A, X], sep: String)(using
    @annotation.unused ev: IsArray[A], pfs: PgTypeFor[String]
  ): TypedExpr[String, X] = {
    val sepFrag = Param.bind[String](sep).fragment
    val s1      = TypedExpr.combineSep(a.fragment, ", ", sepFrag).asInstanceOf[Fragment[X]]
    val frag    = TypedExpr.wrap("array_to_string(", s1, ")")
    TypedExpr[String, X](frag, pg.text)
  }

  def arrayToString[A, X](a: TypedExpr[A, X], sep: String, nullStr: String)(using
    @annotation.unused ev: IsArray[A], pfs: PgTypeFor[String]
  ): TypedExpr[String, X] = {
    val sepFrag  = Param.bind[String](sep).fragment
    val nullFrag = Param.bind[String](nullStr).fragment
    val s1       = TypedExpr.combineSep(a.fragment, ", ", sepFrag).asInstanceOf[Fragment[X]]
    val s2       = TypedExpr.combineSep(s1, ", ", nullFrag).asInstanceOf[Fragment[X]]
    val frag     = TypedExpr.wrap("array_to_string(", s2, ")")
    TypedExpr[String, X](frag, pg.text)
  }

  def stringToArray[X](s: TypedExpr[String, X], sep: String)(using pfs: PgTypeFor[String]): TypedExpr[Arr[String], X] = {
    val sepFrag = Param.bind[String](sep).fragment
    val s1      = TypedExpr.combineSep(s.fragment, ", ", sepFrag).asInstanceOf[Fragment[X]]
    val frag    = TypedExpr.wrap("string_to_array(", s1, ")")
    TypedExpr[Arr[String], X](frag, pg._text)
  }

  def arrayAgg[T, X](expr: TypedExpr[T, X])(using pf: PgTypeFor[Arr[T]]): TypedExpr[Arr[T], X] = {
    val frag = TypedExpr.wrap("array_agg(", expr.fragment, ")")
    TypedExpr[Arr[T], X](frag, pf.codec)
  }

  def unnest[A, E, X](a: TypedExpr[A, X])(using
    @annotation.unused ev: IsArray.Aux[A, E], pf: PgTypeFor[E]
  ): TypedExpr[E, X] = {
    val frag = TypedExpr.wrap("unnest(", a.fragment, ")")
    TypedExpr[E, X](frag, pf.codec)
  }

}
