package skunk.sharp.contrib.pgtrgm

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/**
 * `pg_trgm` — trigram similarity. Functions and operators for fuzzy text search / autocomplete / "did you mean" style
 * matching.
 *
 * Requires `CREATE EXTENSION pg_trgm;` — the validator picks this up if any tag column needs it, but for typical use
 * (function calls + operator extensions on `String`-typed columns), pass `extraExtensions = Set(PgTrgm.RequiredExtension)`
 * to `SchemaValidator.validate`.
 */
trait PgTrgm {

  /** `similarity(a, b)` → Real in [0, 1]. */
  inline def similarity[A, B](
    a: TypedExpr[String, A],
    b: TypedExpr[String, B]
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgFunction.binary[String, String, Float, A, B]("similarity")(a, b)

  /** `word_similarity(a, b)` — similarity of `a` to the closest run of consecutive words in `b`. */
  inline def wordSimilarity[A, B](
    a: TypedExpr[String, A],
    b: TypedExpr[String, B]
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgFunction.binary[String, String, Float, A, B]("word_similarity")(a, b)

  /** `strict_word_similarity(a, b)` — like `word_similarity` but requires whole-word matches. */
  inline def strictWordSimilarity[A, B](
    a: TypedExpr[String, A],
    b: TypedExpr[String, B]
  ): TypedExpr[Float, Where.Concat[A, B]] =
    PgFunction.binary[String, String, Float, A, B]("strict_word_similarity")(a, b)

  /** `show_limit()` — the current similarity threshold (set via `set_limit`). Returns Real. */
  inline def showLimit(): TypedExpr[Float, skunk.Void] =
    PgFunction.nullary[Float]("show_limit")

}

object PgTrgm extends PgTrgm {

  val RequiredExtension: String = "pg_trgm"
}
