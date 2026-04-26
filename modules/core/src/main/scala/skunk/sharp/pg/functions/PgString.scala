package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{Param, PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/** String functions. Mixed into [[skunk.sharp.Pg]]. Args of input expression(s) propagate to result. */
trait PgString {

  // ---- Preserve-tag (T -> T) -----------------------------------------------------------------

  def lower[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("lower",   e)
  def upper[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("upper",   e)
  def trim[T, A](e: TypedExpr[T, A])(using StrLike[T]):  TypedExpr[T, A]   = stringPreserveFn("trim",    e)
  def ltrim[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("ltrim",   e)
  def rtrim[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A]   = stringPreserveFn("rtrim",   e)
  def reverse[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] = stringPreserveFn("reverse", e)
  def initcap[T, A](e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] = stringPreserveFn("initcap", e)

  /** `trim(chars FROM s)`. */
  def trim[T, A](e: TypedExpr[T, A], chars: String)(using StrLike[T], pf: PgTypeFor[String]): TypedExpr[T, A] = {
    val charsFrag = Param.bind[String](chars).fragment
    val inner     = TypedExpr.combineSep(charsFrag, " FROM ", e.fragment).asInstanceOf[Fragment[A]]
    val frag      = TypedExpr.wrap("trim(", inner, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  /** `replace(s, from, to)` with runtime String args (Param.bind). */
  def replace[T, A](e: TypedExpr[T, A], from: String, to: String)(using StrLike[T], pf: PgTypeFor[String]): TypedExpr[T, A] = {
    val fromFrag = Param.bind[String](from).fragment
    val toFrag   = Param.bind[String](to).fragment
    val s1       = TypedExpr.combineSep(e.fragment, ", ", fromFrag).asInstanceOf[Fragment[A]]
    val s2       = TypedExpr.combineSep(s1, ", ", toFrag).asInstanceOf[Fragment[A]]
    val frag     = TypedExpr.wrap("replace(", s2, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  /** `substring(s FROM n)`. */
  def substring[T, A](e: TypedExpr[T, A], from: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val parts = e.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s" FROM $from)"))
    val frag  = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("substring(")) ++ parts,
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `substring(s FROM n FOR m)`. */
  def substring[T, A](e: TypedExpr[T, A], from: Int, forLen: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag  = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("substring(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s" FROM $from FOR $forLen)")),
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `left(s, n)`. */
  def left[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("left(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `right(s, n)`. */
  def right[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("right(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `repeat(s, n)`. */
  def repeat[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("repeat(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `regexp_replace(s, pattern, replacement)`. */
  def regexpReplace[T, A](e: TypedExpr[T, A], pattern: String, replacement: String)(using
    StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val pf1 = Param.bind[String](pattern).fragment
    val pf2 = Param.bind[String](replacement).fragment
    val s1  = TypedExpr.combineSep(e.fragment, ", ", pf1).asInstanceOf[Fragment[A]]
    val s2  = TypedExpr.combineSep(s1, ", ", pf2).asInstanceOf[Fragment[A]]
    val frag = TypedExpr.wrap("regexp_replace(", s2, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  /** `split_part(s, delim, field)`. */
  def splitPart[T, A](e: TypedExpr[T, A], delim: String, field: Int)(using
    StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val delimFrag = Param.bind[String](delim).fragment
    val s1        = TypedExpr.combineSep(e.fragment, ", ", delimFrag).asInstanceOf[Fragment[A]]
    val frag      = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("split_part(")) ++ s1.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $field)")),
      s1.encoder,
      skunk.util.Origin.unknown
    )
    TypedExpr[T, A](frag, e.codec)
  }

  /** `concat(a, b, c, …)` — Args type collapses to `?` since each arg may carry different Args. */
  def concat(args: TypedExpr[String, ?]*): TypedExpr[String, ?] =
    PgFunction.nary[String]("concat", args*)

  // ---- Fixed Int return -----------------------------------------------------------------------

  def length[T, A](e: TypedExpr[T, A])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("length", e)

  def charLength[T, A](e: TypedExpr[T, A])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("char_length", e)

  def octetLength[T, A](e: TypedExpr[T, A])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("octet_length", e)

  /** `position(substr IN str)` — substr is runtime String; nullability tracked via Lift. */
  def position[T, A](substr: String, in: TypedExpr[T, A])(using
    ev: StrLike[T],
    pf: PgTypeFor[Lift[T, Int]],
    pfs: PgTypeFor[String]
  ): TypedExpr[Lift[T, Int], A] = {
    val substrFrag = Param.bind[String](substr).fragment
    val s1         = TypedExpr.combineSep(substrFrag, " IN ", in.fragment).asInstanceOf[Fragment[A]]
    val frag       = TypedExpr.wrap("position(", s1, ")")
    TypedExpr[Lift[T, Int], A](frag, pf.codec)
  }

  // ---- Tag-preserving misc -------------------------------------------------------------------

  def translate[T, A](e: TypedExpr[T, A], from: String, to: String)(using
    StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val ff = Param.bind[String](from).fragment
    val tf = Param.bind[String](to).fragment
    val s1 = TypedExpr.combineSep(e.fragment, ", ", ff).asInstanceOf[Fragment[A]]
    val s2 = TypedExpr.combineSep(s1, ", ", tf).asInstanceOf[Fragment[A]]
    val frag = TypedExpr.wrap("translate(", s2, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  def lpad[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("lpad(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, e.codec)
  }

  def lpad[T, A](e: TypedExpr[T, A], n: Int, fill: String)(using
    StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val fillFrag = Param.bind[String](fill).fragment
    val s1 = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("lpad(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n, ")) ++ fillFrag.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")")),
      e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](s1, e.codec)
  }

  def rpad[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("rpad(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, e.codec)
  }

  def rpad[T, A](e: TypedExpr[T, A], n: Int, fill: String)(using
    StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val fillFrag = Param.bind[String](fill).fragment
    val s1 = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("rpad(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n, ")) ++ fillFrag.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")")),
      e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](s1, e.codec)
  }

  // ---- Fixed text return (NULL-propagating via Lift) ------------------------------------------

  def md5[T, A](e: TypedExpr[T, A])(using StrLike[T], pf: PgTypeFor[Lift[T, String]]): TypedExpr[Lift[T, String], A] = {
    val frag = TypedExpr.wrap("md5(", e.fragment, ")")
    TypedExpr[Lift[T, String], A](frag, pf.codec)
  }

  def chr[T, A](e: TypedExpr[T, A])(using pf: PgTypeFor[Lift[T, String]]): TypedExpr[Lift[T, String], A] = {
    val frag = TypedExpr.wrap("chr(", e.fragment, ")")
    TypedExpr[Lift[T, String], A](frag, pf.codec)
  }

  def toChar[T, A](e: TypedExpr[T, A], fmt: String)(using
    pf: PgTypeFor[Lift[T, String]], pfs: PgTypeFor[String]
  ): TypedExpr[Lift[T, String], A] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val s1      = TypedExpr.combineSep(e.fragment, ", ", fmtFrag).asInstanceOf[Fragment[A]]
    val frag    = TypedExpr.wrap("to_char(", s1, ")")
    TypedExpr[Lift[T, String], A](frag, pf.codec)
  }

  /** `format(fmt, args*)` — collapses Args to `?` because variadic. */
  def format(fmt: String, args: TypedExpr[?, ?]*)(using pf: PgTypeFor[String]): TypedExpr[String, ?] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    if (args.isEmpty) {
      val frag = TypedExpr.wrap("format(", fmtFrag, ")")
      TypedExpr[String, Void](frag, skunk.codec.all.text)
    } else {
      val joined = args.foldLeft(fmtFrag.asInstanceOf[Fragment[Any]]) { (acc, a) =>
        TypedExpr.combineSep(acc, ", ", a.fragment).asInstanceOf[Fragment[Any]]
      }
      val frag = TypedExpr.wrap("format(", joined, ")")
      TypedExpr[String, Any](frag, skunk.codec.all.text)
    }
  }

  // ---- String -> Int -------------------------------------------------------------------------

  def ascii[T, A](e: TypedExpr[T, A])(using StrLike[T], PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("ascii", e)

  // ---- String -> BigDecimal ------------------------------------------------------------------

  def toNumber[T, A](e: TypedExpr[T, A], fmt: String)(using
    StrLike[T], pf: PgTypeFor[Lift[T, BigDecimal]], pfs: PgTypeFor[String]
  ): TypedExpr[Lift[T, BigDecimal], A] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val s1      = TypedExpr.combineSep(e.fragment, ", ", fmtFrag).asInstanceOf[Fragment[A]]
    val frag    = TypedExpr.wrap("to_number(", s1, ")")
    TypedExpr[Lift[T, BigDecimal], A](frag, pf.codec)
  }

}
