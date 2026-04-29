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
  def trim[T, A](e: TypedExpr[T, A], chars: String)(using ev: StrLike[T], pf: PgTypeFor[String]): TypedExpr[T, A] = {
    val charsFrag = Param.bind[String](chars).fragment
    val inner     = TypedExpr.combineSep(charsFrag, " FROM ", e.fragment).asInstanceOf[Fragment[A]]
    val frag      = TypedExpr.wrap("trim(", inner, ")")
    TypedExpr[T, A](frag, e.codec)
  }

  /** `replace(s, from, to)` with runtime String args (Param.bind). */
  def replace[T, A](e: TypedExpr[T, A], from: String, to: String)(using ev: StrLike[T], pf: PgTypeFor[String]): TypedExpr[T, A] = {
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
    ev: StrLike[T], pf: PgTypeFor[String]
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
    ev: StrLike[T], pf: PgTypeFor[String]
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

  /** `concat(a)` — single arg; Args propagates from `a`. */
  def concat[A1](a: TypedExpr[String, A1]): TypedExpr[String, A1] = {
    val frag = TypedExpr.wrap("concat(", a.fragment, ")")
    TypedExpr[String, A1](frag, skunk.codec.all.text)
  }

  /** `concat(a, b)` — Args = `Concat[A1, A2]`. */
  def concat[A1, A2](a: TypedExpr[String, A1], b: TypedExpr[String, A2])(using
    c2: Where.Concat2[A1, A2]
  ): TypedExpr[String, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("concat(", inner, ")")
    TypedExpr[String, Where.Concat[A1, A2]](frag, skunk.codec.all.text)
  }

  /** `concat(a, b, c)` — Args = `Concat[Concat[A1, A2], A3]` (left-fold). */
  def concat[A1, A2, A3](
    a: TypedExpr[String, A1], b: TypedExpr[String, A2], c: TypedExpr[String, A3]
  )(using
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], A3]
  ): TypedExpr[String, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = c123.project(combined)
      val (a1v, a2v) = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment),
      ", ",
      projector
    )
    val frag = TypedExpr.wrap("concat(", combined, ")")
    TypedExpr[String, Where.Concat[Where.Concat[A1, A2], A3]](frag, skunk.codec.all.text)
  }

  /** `concat(a, b, c, d, …)` — variadic fallback for arity > 3; Args = `Void`. */
  def concat(args: TypedExpr[String, ?]*): TypedExpr[String, Void] =
    PgFunction.nary[String]("concat", args*).asInstanceOf[TypedExpr[String, Void]]

  // ---- Fixed Int return -----------------------------------------------------------------------

  def length[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("length", e)

  def charLength[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("char_length", e)

  def octetLength[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
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
    ev: StrLike[T], pf: PgTypeFor[String]
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

  /**
   * `lpad(e, n, fill)` — `n` and `fill` are baked runtime values (Param.bind);  Args propagates
   * from `e` only.
   */
  def lpad[T, A](e: TypedExpr[T, A], n: Int, fill: String)(using
    ev: StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val fillFrag = Param.bind[String](fill).fragment
    // Build parts: lpad( | e.parts | , n, | fill.parts | )
    val parts =
      List[Either[String, cats.data.State[Int, String]]](Left("lpad(")) ++
        e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n, ")) ++
        fillFrag.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    // Encoder: e.encoder takes A; fill encoder takes Void (baked). Combine left-Void.
    val combinedEnc = TypedExpr.combineEnc[A, Void](e.fragment.encoder, fillFrag.encoder)(using Where.Concat2.rightVoid[A])
    val frag        = Fragment(parts, combinedEnc.asInstanceOf[skunk.Encoder[A]], skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, e.codec)
  }

  def rpad[T, A](e: TypedExpr[T, A], n: Int)(using StrLike[T]): TypedExpr[T, A] = {
    val frag = Fragment[A](
      List[Either[String, cats.data.State[Int, String]]](Left("rpad(")) ++ e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n)")),
      e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, e.codec)
  }

  /**
   * `rpad(e, n, fill)` — `n` and `fill` are baked runtime values (Param.bind); Args propagates
   * from `e` only.
   */
  def rpad[T, A](e: TypedExpr[T, A], n: Int, fill: String)(using
    ev: StrLike[T], pf: PgTypeFor[String]
  ): TypedExpr[T, A] = {
    val fillFrag = Param.bind[String](fill).fragment
    val parts =
      List[Either[String, cats.data.State[Int, String]]](Left("rpad(")) ++
        e.fragment.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(s", $n, ")) ++
        fillFrag.parts ++
        List[Either[String, cats.data.State[Int, String]]](Left(")"))
    val combinedEnc = TypedExpr.combineEnc[A, Void](e.fragment.encoder, fillFrag.encoder)(using Where.Concat2.rightVoid[A])
    val frag        = Fragment(parts, combinedEnc.asInstanceOf[skunk.Encoder[A]], skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, e.codec)
  }

  // ---- Fixed text return (NULL-propagating via Lift) ------------------------------------------

  def md5[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, String]]): TypedExpr[Lift[T, String], A] = {
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

  /** `format(fmt, args*)` — variadic; Args = Void (all inputs treated as Void-args). */
  def format(fmt: String, args: TypedExpr[?, ?]*)(using pf: PgTypeFor[String]): TypedExpr[String, Void] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val joined  = TypedExpr.joinedVoid(", ", fmtFrag :: args.toList.map(_.fragment))
    val frag    = TypedExpr.wrap("format(", joined, ")")
    TypedExpr[String, Void](frag, skunk.codec.all.text)
  }

  // ---- String -> Int -------------------------------------------------------------------------

  def ascii[T, A](e: TypedExpr[T, A])(using ev: StrLike[T], pf: PgTypeFor[Lift[T, Int]]): TypedExpr[Lift[T, Int], A] =
    stringToIntFn("ascii", e)

  // ---- String -> BigDecimal ------------------------------------------------------------------

  def toNumber[T, A](e: TypedExpr[T, A], fmt: String)(using
    ev: StrLike[T], pf: PgTypeFor[Lift[T, BigDecimal]], pfs: PgTypeFor[String]
  ): TypedExpr[Lift[T, BigDecimal], A] = {
    val fmtFrag = Param.bind[String](fmt).fragment
    val s1      = TypedExpr.combineSep(e.fragment, ", ", fmtFrag).asInstanceOf[Fragment[A]]
    val frag    = TypedExpr.wrap("to_number(", s1, ")")
    TypedExpr[Lift[T, BigDecimal], A](frag, pf.codec)
  }

}
