package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/**
 * String functions. Mixed into [[skunk.sharp.Pg]]. Args of input expression(s) propagate to result.
 *
 * Every value-arg is a `TypedExpr[T, Args]`: callers pass column refs, `Param[T]` (deferred), `lit(v)` (compile-time
 * literal), or `Param.bind(v)` (explicit bake) — same static-by-default rule the operator positions enforce. The result
 * `Args` is the smart-flat concat over every input's `Args` (see [[Where.FoldConcat]]).
 */
trait PgString {

  // ---- Preserve-tag, single-arg (T -> T) -------------------------------------------------------

  def lower[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("lower", e)
  def upper[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("upper", e)
  def trim[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]    = stringPreserveFn("trim", e)
  def ltrim[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("ltrim", e)
  def rtrim[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("rtrim", e)
  def reverse[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] = stringPreserveFn("reverse", e)
  def initcap[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] = stringPreserveFn("initcap", e)

  // ---- Tag-preserving multi-arg (T -> T) -------------------------------------------------------

  /** `trim(chars FROM s)`. */
  inline def trim[T, A1, A2](
    chars: TypedExpr[String, A1],
    e: TypedExpr[T, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](chars.fragment, " FROM ", e.fragment)
    val frag  = TypedExpr.wrap("trim(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `replace(s, from, to)`. */
  inline def replace[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    from: TypedExpr[String, A2],
    to: TypedExpr[String, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "replace",
      List(e.fragment, from.fragment, to.fragment),
      e.codec
    )

  /** `substring(s FROM n)`. */
  inline def substring[T, A1, A2](
    e: TypedExpr[T, A1],
    from: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, " FROM ", from.fragment)
    val frag  = TypedExpr.wrap("substring(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `substring(s FROM n FOR m)`. */
  inline def substring[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    from: TypedExpr[Int, A2],
    forLen: TypedExpr[Int, A3]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val ab   = TypedExpr.combineSepInl[A1, A2](e.fragment, " FROM ", from.fragment)
    val abc  = TypedExpr.combineSepInl[Where.Concat[A1, A2], A3](ab, " FOR ", forLen.fragment)
    val frag = TypedExpr.wrap("substring(", abc, ")")
    TypedExpr(frag, e.codec)
  }

  /** `left(s, n)`. */
  inline def left[T, A1, A2](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("left(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `right(s, n)`. */
  inline def right[T, A1, A2](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("right(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `repeat(s, n)`. */
  inline def repeat[T, A1, A2](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("repeat(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `regexp_replace(s, pattern, replacement)`. */
  inline def regexpReplace[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    pattern: TypedExpr[String, A2],
    replacement: TypedExpr[String, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "regexp_replace",
      List(e.fragment, pattern.fragment, replacement.fragment),
      e.codec
    )

  /** `split_part(s, delim, field)`. */
  inline def splitPart[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    delim: TypedExpr[String, A2],
    field: TypedExpr[Int, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "split_part",
      List(e.fragment, delim.fragment, field.fragment),
      e.codec
    )

  /** `concat(a)` — single arg; Args propagates from `a`. */
  def concat[A1](a: TypedExpr[String, A1]): TypedExpr[String, A1] = {
    val frag = TypedExpr.wrap("concat(", a.fragment, ")")
    TypedExpr[String, A1](frag, skunk.codec.all.text)
  }

  /** `concat(a, b)` — Args = `Concat[A1, A2]`. */
  inline def concat[A1, A2](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2]
  ): TypedExpr[String, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("concat(", inner, ")")
    TypedExpr[String, Where.Concat[A1, A2]](frag, skunk.codec.all.text)
  }

  /** `concat(a, b, c)` — `Args` flattens to the non-Void slots of `(A1…A3)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment),
      skunk.codec.all.text
    )

  /** `concat(a, b, c, d)` — `Args` flattens to the non-Void slots of `(A1…A4)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3, A4](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment),
      skunk.codec.all.text
    )

  /** `concat(a, b, c, d, e)` — `Args` flattens to the non-Void slots of `(A1…A5)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3, A4, A5](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4],
    e: TypedExpr[String, A5]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment),
      skunk.codec.all.text
    )

  /** `concat(a, b, c, d, e, f)` — `Args` flattens to the non-Void slots of `(A1…A6)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3, A4, A5, A6](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4],
    e: TypedExpr[String, A5],
    f: TypedExpr[String, A6]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment),
      skunk.codec.all.text
    )

  /** `concat(a, b, c, d, e, f, g)` — `Args` flattens to the non-Void slots of `(A1…A7)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3, A4, A5, A6, A7](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4],
    e: TypedExpr[String, A5],
    f: TypedExpr[String, A6],
    g: TypedExpr[String, A7]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment),
      skunk.codec.all.text
    )

  /** `concat(a, b, c, d, e, f, g, h)` — `Args` flattens to the non-Void slots of `(A1…A8)` via `Where.FoldConcat`. */
  inline def concat[A1, A2, A3, A4, A5, A6, A7, A8](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4],
    e: TypedExpr[String, A5],
    f: TypedExpr[String, A6],
    g: TypedExpr[String, A7],
    h: TypedExpr[String, A8]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment),
      skunk.codec.all.text
    )

  /**
   * `concat(a, b, c, d, e, f, g, h, i)` — `Args` flattens to the non-Void slots of `(A1…A9)` via `Where.FoldConcat`.
   */
  inline def concat[A1, A2, A3, A4, A5, A6, A7, A8, A9](
    a: TypedExpr[String, A1],
    b: TypedExpr[String, A2],
    c: TypedExpr[String, A3],
    d: TypedExpr[String, A4],
    e: TypedExpr[String, A5],
    f: TypedExpr[String, A6],
    g: TypedExpr[String, A7],
    h: TypedExpr[String, A8],
    i: TypedExpr[String, A9]
  ): TypedExpr[String, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple]] =
    PgFunction.naryTypedFold[String, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple](
      "concat",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment, i.fragment),
      skunk.codec.all.text
    )

  // ---- Fixed Int return -----------------------------------------------------------------------

  def length[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("length", e)

  def charLength[T, A](e: TypedExpr[T, A])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("char_length", e)

  def octetLength[T, A](e: TypedExpr[T, A])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("octet_length", e)

  /** `position(substr IN str)` — nullability tracked via Lift on `in`. */
  inline def position[T, A1, A2](
    substr: TypedExpr[String, A1],
    in: TypedExpr[T, A2]
  )(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](substr.fragment, " IN ", in.fragment)
    val frag  = TypedExpr.wrap("position(", inner, ")")
    TypedExpr(frag, pf.codec)
  }

  // ---- Tag-preserving misc -------------------------------------------------------------------

  /** `translate(s, from, to)`. */
  inline def translate[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    from: TypedExpr[String, A2],
    to: TypedExpr[String, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "translate",
      List(e.fragment, from.fragment, to.fragment),
      e.codec
    )

  /** `lpad(s, n)`. */
  inline def lpad[T, A1, A2](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("lpad(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `lpad(s, n, fill)`. */
  inline def lpad[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2],
    fill: TypedExpr[String, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "lpad",
      List(e.fragment, n.fragment, fill.fragment),
      e.codec
    )

  /** `rpad(s, n)`. */
  inline def rpad[T, A1, A2](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2]
  )(using StrLike[T]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("rpad(", inner, ")")
    TypedExpr(frag, e.codec)
  }

  /** `rpad(s, n, fill)`. */
  inline def rpad[T, A1, A2, A3](
    e: TypedExpr[T, A1],
    n: TypedExpr[Int, A2],
    fill: TypedExpr[String, A3]
  )(using StrLike[T]): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "rpad",
      List(e.fragment, n.fragment, fill.fragment),
      e.codec
    )

  // ---- Fixed text return (NULL-propagating via Lift) ------------------------------------------

  def md5[T, A](e: TypedExpr[T, A])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, String]]
  ): TypedExpr[Lift[T, String], A] = {
    val frag = TypedExpr.wrap("md5(", e.fragment, ")")
    TypedExpr[Lift[T, String], A](frag, pf.codec)
  }

  def chr[T, A](e: TypedExpr[T, A])(using pf: PgTypeFor[Lift[T, String]]): TypedExpr[Lift[T, String], A] = {
    val frag = TypedExpr.wrap("chr(", e.fragment, ")")
    TypedExpr[Lift[T, String], A](frag, pf.codec)
  }

  /** `to_char(e, fmt)` — result is `Lift[T, String]`. */
  inline def toChar[T, A1, A2](
    e: TypedExpr[T, A1],
    fmt: TypedExpr[String, A2]
  )(using pf: PgTypeFor[Lift[T, String]]): TypedExpr[Lift[T, String], Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", fmt.fragment)
    val frag  = TypedExpr.wrap("to_char(", inner, ")")
    TypedExpr(frag, pf.codec)
  }

  /**
   * `format(fmt, args*)` — variadic. Result `Args = Void`: every input is treated as Void-args by
   * [[TypedExpr.joinedVoid]], so any `Param[T]` baked into a spliced fragment must already be a `Param.bind`-style
   * Void-args fragment. Threading typed `Args` through the variadic shape is a roadmap item.
   */
  def format(fmt: TypedExpr[String, ?], args: TypedExpr[?, ?]*): TypedExpr[String, skunk.Void] = {
    val joined = TypedExpr.joinedVoid(", ", fmt.fragment :: args.toList.map(_.fragment))
    val frag   = TypedExpr.wrap("format(", joined, ")")
    TypedExpr[String, skunk.Void](frag, skunk.codec.all.text)
  }

  // ---- String -> Int -------------------------------------------------------------------------

  def ascii[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("ascii", e)

  // ---- String -> BigDecimal ------------------------------------------------------------------

  /** `to_number(e, fmt)`. */
  inline def toNumber[T, A1, A2](
    e: TypedExpr[T, A1],
    fmt: TypedExpr[String, A2]
  )(using ev: StrLike[T], pf: PgTypeFor[Lift[T, BigDecimal]]): TypedExpr[Lift[T, BigDecimal], Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](e.fragment, ", ", fmt.fragment)
    val frag  = TypedExpr.wrap("to_number(", inner, ")")
    TypedExpr(frag, pf.codec)
  }

}
