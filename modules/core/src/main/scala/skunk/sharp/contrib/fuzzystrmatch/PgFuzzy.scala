package skunk.sharp.contrib.fuzzystrmatch

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/**
 * `fuzzystrmatch` — string-distance / phonetic functions. Complements [[skunk.sharp.contrib.pgtrgm]]: trigrams handle
 * substring similarity, fuzzystrmatch handles edit-distance and sound-alike matching.
 *
 * Function-only module — no tag types. Pass `extraExtensions = Set(PgFuzzy.RequiredExtension)` to
 * `SchemaValidator.validate` when you want the missing-extension check.
 */
trait PgFuzzy {

  /** `levenshtein(a, b)` — edit distance. */
  inline def levenshtein[A, B](
    a: TypedExpr[String, A],
    b: TypedExpr[String, B]
  ): TypedExpr[Int, Where.Concat[A, B]] =
    PgFunction.binary[String, String, Int, A, B]("levenshtein")(a, b)

  /** `levenshtein(a, b, ins_cost, del_cost, sub_cost)` — weighted edit distance. */
  inline def levenshtein[A, B, C, D, E](
    a: TypedExpr[String, A],
    b: TypedExpr[String, B],
    insCost: TypedExpr[Int, C],
    delCost: TypedExpr[Int, D],
    subCost: TypedExpr[Int, E]
  ): TypedExpr[Int, Where.Concat[A, Where.Concat[B, Where.Concat[C, Where.Concat[D, E]]]]] = {
    val deTail = TypedExpr.combineSepInl[D, E](delCost.fragment, ", ", subCost.fragment)
    val cdeTail = TypedExpr.combineSepInl[C, Where.Concat[D, E]](insCost.fragment, ", ", deTail)
    val bcdeTail = TypedExpr.combineSepInl[B, Where.Concat[C, Where.Concat[D, E]]](b.fragment, ", ", cdeTail)
    val inner = TypedExpr
      .combineSepInl[A, Where.Concat[B, Where.Concat[C, Where.Concat[D, E]]]](a.fragment, ", ", bcdeTail)
    val frag = TypedExpr.wrap("levenshtein(", inner, ")")
    TypedExpr[Int, Where.Concat[A, Where.Concat[B, Where.Concat[C, Where.Concat[D, E]]]]](frag, skunk.codec.all.int4)
  }

  /** `soundex(s)` — 4-character Soundex code. */
  inline def soundex[A](s: TypedExpr[String, A]): TypedExpr[String, A] =
    PgFunction.unary[String, String, A]("soundex")(s)

  /** `metaphone(s, maxLen)` — phonetic code, length-capped. */
  inline def metaphone[A, B](
    s: TypedExpr[String, A],
    maxLen: TypedExpr[Int, B]
  ): TypedExpr[String, Where.Concat[A, B]] =
    PgFunction.binary[String, Int, String, A, B]("metaphone")(s, maxLen)

  /** `dmetaphone(s)` — double-metaphone primary code. */
  inline def dmetaphone[A](s: TypedExpr[String, A]): TypedExpr[String, A] =
    PgFunction.unary[String, String, A]("dmetaphone")(s)

  /** `dmetaphone_alt(s)` — double-metaphone alternate code. */
  inline def dmetaphoneAlt[A](s: TypedExpr[String, A]): TypedExpr[String, A] =
    PgFunction.unary[String, String, A]("dmetaphone_alt")(s)

}

object PgFuzzy extends PgFuzzy {

  val RequiredExtension: String = "fuzzystrmatch"
}
