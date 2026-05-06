package skunk.sharp.pg.functions

import skunk.codec.all as pg
import skunk.data.Arr
import skunk.sharp.TypedExpr
import skunk.sharp.pg.{IsArray, PgTypeFor}
import skunk.sharp.where.Where

/**
 * Built-in Postgres array functions. Operators (`@>`, `<@`, `&&`, `||`, `= ANY`) live as extensions in
 * [[skunk.sharp.pg.ArrayOps]]. Args of input expression(s) propagate to result.
 */
trait PgArray {

  inline def arrayLength[A, X, Y](
    a:   TypedExpr[A, X],
    dim: TypedExpr[Int, Y]
  )(using @annotation.unused ev: IsArray[A]): TypedExpr[Option[Int], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", dim.fragment)
    val frag  = TypedExpr.wrap("array_length(", inner, ")")
    TypedExpr(frag, pg.int4.opt)
  }

  def cardinality[A, X](a: TypedExpr[A, X])(using @annotation.unused ev: IsArray[A]): TypedExpr[Int, X] = {
    val frag = TypedExpr.wrap("cardinality(", a.fragment, ")")
    TypedExpr[Int, X](frag, pg.int4)
  }

  def arrayAppend[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment, _.asInstanceOf[(X, Y)])
    val frag  = TypedExpr.wrap("array_append(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayPrepend[A, E, X, Y](elem: TypedExpr[E, X], a: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(elem.fragment, ", ", a.fragment, _.asInstanceOf[(X, Y)])
    val frag  = TypedExpr.wrap("array_prepend(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayCat[A, X, Y](a: TypedExpr[A, X], b: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray[A]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment, _.asInstanceOf[(X, Y)])
    val frag  = TypedExpr.wrap("array_cat(", inner, ")")
    TypedExpr[A, Where.Concat[X, Y]](frag, a.codec)
  }

  def arrayPosition[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Option[Int], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment, _.asInstanceOf[(X, Y)])
    val frag  = TypedExpr.wrap("array_position(", inner, ")")
    TypedExpr[Option[Int], Where.Concat[X, Y]](frag, pg.int4.opt)
  }

  def arrayPositions[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Arr[Int], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment, _.asInstanceOf[(X, Y)])
    val frag  = TypedExpr.wrap("array_positions(", inner, ")")
    TypedExpr[Arr[Int], Where.Concat[X, Y]](frag, pg._int4)
  }

  def arrayRemove[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", elem.fragment, _.asInstanceOf[(X, Y)])
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

  inline def arrayToString[A, X, Y](
    a:   TypedExpr[A, X],
    sep: TypedExpr[String, Y]
  )(using @annotation.unused ev: IsArray[A]): TypedExpr[String, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", sep.fragment)
    val frag  = TypedExpr.wrap("array_to_string(", inner, ")")
    TypedExpr(frag, pg.text)
  }

  import skunk.sharp.PgFunction

  inline def arrayToString[A, X, Y, Z](
    a:       TypedExpr[A, X],
    sep:     TypedExpr[String, Y],
    nullStr: TypedExpr[String, Z]
  )(using @annotation.unused ev: IsArray[A]): TypedExpr[String, Where.FoldConcat[X *: Y *: Z *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, X *: Y *: Z *: EmptyTuple](
      "array_to_string", List(a.fragment, sep.fragment, nullStr.fragment), pg.text
    )

  inline def stringToArray[X, Y](
    s:   TypedExpr[String, X],
    sep: TypedExpr[String, Y]
  ): TypedExpr[Arr[String], Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](s.fragment, ", ", sep.fragment)
    val frag  = TypedExpr.wrap("string_to_array(", inner, ")")
    TypedExpr(frag, pg._text)
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
