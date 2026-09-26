package skunk.sharp.pg.functions

import skunk.codec.all as pg
import skunk.data.Arr
import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.{IsArray, PgTypeFor}
import skunk.sharp.where.Where

/**
 * Built-in Postgres array functions. Operators (`@>`, `<@`, `&&`, `||`, `= ANY`) live as extensions in
 * [[skunk.sharp.pg.ArrayOps]]. Args of input expression(s) propagate to result.
 */
trait PgArray {

  inline def arrayLength[A, X, Y](
    a: TypedExpr[A, X],
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

  inline def arrayAppend[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] =
    PgFunction.call2("array_append", a, elem, a.codec)

  inline def arrayPrepend[A, E, X, Y](elem: TypedExpr[E, X], a: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] =
    PgFunction.call2("array_prepend", elem, a, a.codec)

  inline def arrayCat[A, X, Y](a: TypedExpr[A, X], b: TypedExpr[A, Y])(using
    @annotation.unused ev: IsArray[A]
  ): TypedExpr[A, Where.Concat[X, Y]] =
    PgFunction.call2("array_cat", a, b, a.codec)

  inline def arrayPosition[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Option[Int], Where.Concat[X, Y]] =
    PgFunction.call2("array_position", a, elem, pg.int4.opt)

  inline def arrayPositions[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[Arr[Int], Where.Concat[X, Y]] =
    PgFunction.call2("array_positions", a, elem, pg._int4)

  inline def arrayRemove[A, E, X, Y](a: TypedExpr[A, X], elem: TypedExpr[E, Y])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.Concat[X, Y]] =
    PgFunction.call2("array_remove", a, elem, a.codec)

  inline def arrayReplace[A, E, X, Y, Z](a: TypedExpr[A, X], from: TypedExpr[E, Y], to: TypedExpr[E, Z])(using
    @annotation.unused ev: IsArray.Aux[A, E]
  ): TypedExpr[A, Where.FoldConcat[(X, Y, Z)]] =
    PgFunction.naryTypedFold[A, (X, Y, Z)]("array_replace", List(a.fragment, from.fragment, to.fragment), a.codec)

  inline def arrayToString[A, X, Y](
    a: TypedExpr[A, X],
    sep: TypedExpr[String, Y]
  )(using @annotation.unused ev: IsArray[A]): TypedExpr[String, Where.Concat[X, Y]] = {
    val inner = TypedExpr.combineSepInl[X, Y](a.fragment, ", ", sep.fragment)
    val frag  = TypedExpr.wrap("array_to_string(", inner, ")")
    TypedExpr(frag, pg.text)
  }

  import skunk.sharp.PgFunction

  inline def arrayToString[A, X, Y, Z](
    a: TypedExpr[A, X],
    sep: TypedExpr[String, Y],
    nullStr: TypedExpr[String, Z]
  )(using @annotation.unused ev: IsArray[A]): TypedExpr[String, Where.FoldConcat[X *: Y *: Z *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, X *: Y *: Z *: EmptyTuple](
      "array_to_string",
      List(a.fragment, sep.fragment, nullStr.fragment),
      pg.text
    )

  inline def stringToArray[X, Y](
    s: TypedExpr[String, X],
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
    @annotation.unused ev: IsArray.Aux[A, E],
    pf: PgTypeFor[E]
  ): TypedExpr[E, X] = {
    val frag = TypedExpr.wrap("unnest(", a.fragment, ")")
    TypedExpr[E, X](frag, pf.codec)
  }

}
