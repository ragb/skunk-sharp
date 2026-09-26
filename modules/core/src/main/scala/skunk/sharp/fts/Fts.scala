package skunk.sharp.fts

import skunk.Fragment
import skunk.sharp.TypedExpr
import skunk.sharp.ops.Stripped
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.pg.functions.{Lift, StrLike}
import skunk.sharp.where.Where

/**
 * Full-text search functions. The optional `config` argument (`"english"`, `"simple"`, …) is cast to `regconfig`, so it
 * can be a literal or a `Param[String]`; without it Postgres uses `default_text_search_config`.
 *
 * {{{
 *   import skunk.sharp.fts.*
 *   docs.select(d => (d.id, Fts.tsRank(d.tsv, Fts.websearchToTsQuery("english", Param.named["q", String]))))
 *     .where(d => d.tsv.matches(Fts.websearchToTsQuery("english", Param.named["q", String])))
 * }}}
 */
object Fts {

  private def regconfig[A](config: TypedExpr[String, A]): Fragment[A] =
    TypedExpr.wrap("", config.fragment, "::regconfig")

  private inline def call1[R, A](name: String, a: Fragment[A])(using pf: PgTypeFor[R]): TypedExpr[R, A] =
    TypedExpr[R, A](TypedExpr.wrap(s"$name(", a, ")"), pf.codec)

  private inline def call2[R, A, B](name: String, a: Fragment[A], b: Fragment[B])(using
    pf: PgTypeFor[R]
  ): TypedExpr[R, Where.Concat[A, B]] =
    TypedExpr[R, Where.Concat[A, B]](
      TypedExpr.wrap(s"$name(", TypedExpr.combineSepInl[A, B](a, ", ", b), ")"),
      pf.codec
    )

  private inline def call3[R, A, B, C](name: String, a: Fragment[A], b: Fragment[B], c: Fragment[C])(using
    pf: PgTypeFor[R]
  ): TypedExpr[R, Where.Concat[Where.Concat[A, B], C]] = {
    val ab = TypedExpr.combineSepInl[A, B](a, ", ", b)
    TypedExpr[R, Where.Concat[Where.Concat[A, B], C]](
      TypedExpr.wrap(s"$name(", TypedExpr.combineSepInl[Where.Concat[A, B], C](ab, ", ", c), ")"),
      pf.codec
    )
  }

  // ---- Documents ---------------------------------------------------------------------------------

  /** `to_tsvector(doc)` — parse a document with the default configuration. NULL in, NULL out. */
  inline def toTsVector[T, A](doc: TypedExpr[T, A])(using
    StrLike[T],
    PgTypeFor[Lift[T, TsVector]]
  )
    : TypedExpr[Lift[T, TsVector], A] =
    call1[Lift[T, TsVector], A]("to_tsvector", doc.fragment)

  /** `to_tsvector(config, doc)`. */
  inline def toTsVector[C, T, A](config: TypedExpr[String, C], doc: TypedExpr[T, A])(using
    StrLike[T],
    PgTypeFor[Lift[T, TsVector]]
  ): TypedExpr[Lift[T, TsVector], Where.Concat[C, A]] =
    call2[Lift[T, TsVector], C, A]("to_tsvector", regconfig(config), doc.fragment)

  /** `setweight(doc, 'A')` — label a document's lexemes with a weight (`A` highest … `D`), for ranking. */
  inline def setWeight[T, A](doc: TypedExpr[T, A], weight: "A" | "B" | "C" | "D")(using
    Stripped[T] <:< TsVector,
    PgTypeFor[Lift[T, TsVector]]
  ): TypedExpr[Lift[T, TsVector], A] =
    TypedExpr[Lift[T, TsVector], A](
      TypedExpr.wrap("setweight(", doc.fragment, s", '$weight')"),
      summon[PgTypeFor[Lift[T, TsVector]]].codec
    )

  // ---- Queries -----------------------------------------------------------------------------------

  /** `to_tsquery(query)` — operator syntax (`fat & (rat | cat)`); errors on malformed input. */
  inline def toTsQuery[A](query: TypedExpr[String, A]): TypedExpr[TsQuery, A] =
    call1[TsQuery, A]("to_tsquery", query.fragment)

  inline def toTsQuery[C, A](
    config: TypedExpr[String, C],
    query: TypedExpr[String, A]
  ): TypedExpr[TsQuery, Where.Concat[C, A]] =
    call2[TsQuery, C, A]("to_tsquery", regconfig(config), query.fragment)

  /** `plainto_tsquery(text)` — all words ANDed; punctuation ignored. */
  inline def plainToTsQuery[A](text: TypedExpr[String, A]): TypedExpr[TsQuery, A] =
    call1[TsQuery, A]("plainto_tsquery", text.fragment)

  inline def plainToTsQuery[C, A](
    config: TypedExpr[String, C],
    text: TypedExpr[String, A]
  ): TypedExpr[TsQuery, Where.Concat[C, A]] =
    call2[TsQuery, C, A]("plainto_tsquery", regconfig(config), text.fragment)

  /** `phraseto_tsquery(text)` — the words as a phrase (`<->` between them). */
  inline def phraseToTsQuery[A](text: TypedExpr[String, A]): TypedExpr[TsQuery, A] =
    call1[TsQuery, A]("phraseto_tsquery", text.fragment)

  inline def phraseToTsQuery[C, A](
    config: TypedExpr[String, C],
    text: TypedExpr[String, A]
  ): TypedExpr[TsQuery, Where.Concat[C, A]] =
    call2[TsQuery, C, A]("phraseto_tsquery", regconfig(config), text.fragment)

  /**
   * `websearch_to_tsquery(text)` — search-box syntax (`"exact phrase" -excluded or alternative`). Never errors on
   * malformed input, so it's the one to use for user-typed queries.
   */
  inline def websearchToTsQuery[A](text: TypedExpr[String, A]): TypedExpr[TsQuery, A] =
    call1[TsQuery, A]("websearch_to_tsquery", text.fragment)

  inline def websearchToTsQuery[C, A](
    config: TypedExpr[String, C],
    text: TypedExpr[String, A]
  ): TypedExpr[TsQuery, Where.Concat[C, A]] =
    call2[TsQuery, C, A]("websearch_to_tsquery", regconfig(config), text.fragment)

  // ---- Ranking / highlighting --------------------------------------------------------------------

  /** `ts_rank(doc, query)` — relevance by lexeme frequency (`real`). */
  inline def tsRank[T, A, B](doc: TypedExpr[T, A], query: TypedExpr[TsQuery, B])(using
    Stripped[T] <:< TsVector
  ): TypedExpr[Float, Where.Concat[A, B]] =
    call2[Float, A, B]("ts_rank", doc.fragment, query.fragment)

  /** `ts_rank_cd(doc, query)` — cover density: also rewards matched terms being close together. */
  inline def tsRankCd[T, A, B](doc: TypedExpr[T, A], query: TypedExpr[TsQuery, B])(using
    Stripped[T] <:< TsVector
  ): TypedExpr[Float, Where.Concat[A, B]] =
    call2[Float, A, B]("ts_rank_cd", doc.fragment, query.fragment)

  /** `ts_headline(text, query)` — the text with matches highlighted (`<b>…</b>` by default), for result snippets. */
  inline def tsHeadline[A, B](
    text: TypedExpr[String, A],
    query: TypedExpr[TsQuery, B]
  ): TypedExpr[String, Where.Concat[A, B]] =
    call2[String, A, B]("ts_headline", text.fragment, query.fragment)

  inline def tsHeadline[C, A, B](
    config: TypedExpr[String, C],
    text: TypedExpr[String, A],
    query: TypedExpr[TsQuery, B]
  ): TypedExpr[String, Where.Concat[Where.Concat[C, A], B]] =
    call3[String, C, A, B]("ts_headline", regconfig(config), text.fragment, query.fragment)

}
