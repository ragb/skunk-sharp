package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor

/**
 * String functions. Mixed into [[skunk.sharp.Pg]].
 *
 * Every function's input is any `T` where `Stripped[T] <:< String` (see [[StrLike]]): plain `String`, string tag types
 * (`Varchar[N]`, `Bpchar[N]`, `Text`), and their `Option` variants. Return types split into two groups:
 *
 *   - **Preserve the input tag** (`lower`, `upper`, `trim`, `replace`, `substring`, …). `PG returns `text` on the wire
 *     but `Varchar[N]` / `Text` etc. all decode the same text bytes through the same codec, so the value is correct and
 *     the Scala type keeps tag info.
 *   - **Fixed `Int` return** (`length`, `charLength`, `octetLength`, `position`). Wrapped in [[Lift]] so nullable
 *     inputs stay nullable — `length(TypedExpr[Option[String]]) → TypedExpr[Option[Int]]`.
 */
trait PgString {

  // -------- Preserve input tag ------------------------------------------------------------------

  /** `lower(s)` — fold to lowercase. */
  def lower[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("lower", e)

  /** `upper(s)` — fold to uppercase. */
  def upper[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("upper", e)

  /** `trim(s)` — strip leading / trailing whitespace. */
  def trim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("trim", e)

  /** `trim(chars FROM s)` — strip any of `chars` from both ends. */
  def trim[T](e: TypedExpr[T], chars: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("trim(") |+| TypedExpr.parameterised(chars).render |+| TypedExpr.raw(" FROM ") |+| e.render |+|
        TypedExpr.raw(")"),
      e.codec
    )

  /** `ltrim(s)`. */
  def ltrim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("ltrim", e)

  /** `rtrim(s)`. */
  def rtrim[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("rtrim", e)

  /** `replace(s, from, to)`. */
  def replace[T](e: TypedExpr[T], from: String, to: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("replace(") |+| e.render |+| TypedExpr.raw(", ") |+| TypedExpr.parameterised(from).render |+|
        TypedExpr.raw(", ") |+| TypedExpr.parameterised(to).render |+| TypedExpr.raw(")"),
      e.codec
    )

  /** `substring(s FROM n)` — 1-indexed start, no length cap. */
  def substring[T](e: TypedExpr[T], from: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("substring(") |+| e.render |+| TypedExpr.raw(s" FROM $from)"), e.codec)

  /** `substring(s FROM n FOR m)` — 1-indexed start, `m` characters. */
  def substring[T](e: TypedExpr[T], from: Int, forLen: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("substring(") |+| e.render |+| TypedExpr.raw(s" FROM $from FOR $forLen)"), e.codec)

  /** `left(s, n)` — first `n` chars (negative `n` drops the last `|n|`). */
  def left[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("left(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `right(s, n)` — last `n` chars. */
  def right[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("right(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `repeat(s, n)`. */
  def repeat[T](e: TypedExpr[T], n: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(TypedExpr.raw("repeat(") |+| e.render |+| TypedExpr.raw(s", $n)"), e.codec)

  /** `reverse(s)`. */
  def reverse[T](e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] = stringPreserveFn("reverse", e)

  /** `regexp_replace(s, pattern, replacement)`. */
  def regexpReplace[T](e: TypedExpr[T], pattern: String, replacement: String)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("regexp_replace(") |+| e.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.parameterised(pattern).render |+| TypedExpr.raw(", ") |+|
        TypedExpr.parameterised(replacement).render |+|
        TypedExpr.raw(")"),
      e.codec
    )

  /** `split_part(s, delim, field)`. */
  def splitPart[T](e: TypedExpr[T], delim: String, field: Int)(using StrLike[T]): TypedExpr[T] =
    TypedExpr(
      TypedExpr.raw("split_part(") |+| e.render |+| TypedExpr.raw(", ") |+|
        TypedExpr.parameterised(delim).render |+| TypedExpr.raw(s", $field)"),
      e.codec
    )

  /** `concat(a, b, c, …)`. All args expected as `TypedExpr[String]`. */
  def concat(args: TypedExpr[String]*): TypedExpr[String] =
    PgFunction.nary[String]("concat", args*)

  // -------- Fixed `Int` return (NULL-propagating) -----------------------------------------------

  /** `length(s)` — character count. */
  def length[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("length", e)

  /** `char_length(s)` — synonym of [[length]]. */
  def charLength[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("char_length", e)

  /** `octet_length(s)` — byte count. */
  def octetLength[T](e: TypedExpr[T])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int]] =
    stringToIntFn("octet_length", e)

  /** `position(substr IN str)` — 1-indexed; 0 if not found, NULL if `str` is NULL. */
  def position[T](substr: String, in: TypedExpr[T])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]]
  ): TypedExpr[Lift[T, Int]] =
    TypedExpr(
      TypedExpr.raw("position(") |+| TypedExpr.parameterised(substr).render |+| TypedExpr.raw(" IN ") |+|
        in.render |+| TypedExpr.raw(")"),
      pf.codec
    )

}
