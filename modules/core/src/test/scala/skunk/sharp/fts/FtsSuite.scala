package skunk.sharp.fts

import skunk.sharp.dsl.*

object FtsSuite {
  case class Doc(id: Int, title: String, body: String, tsv: Option[TsVector])
  val docs = Table.of[Doc]("docs").withPrimary("id").withDefault("id").withGenerated("tsv")
}

/** Outside `skunk.sharp.dsl` on purpose: this is how user code sees the fts surface. */
class FtsSuite extends munit.FunSuite {
  import FtsSuite.*

  test("to_tsvector with and without a config; the config is cast to regconfig") {
    val q = docs.select(d => (Fts.toTsVector(d.body), Fts.toTsVector("english", d.body))).compile
    assertEquals(
      q.fragment.sql,
      """SELECT to_tsvector("body"), to_tsvector('english'::regconfig, "body") FROM "docs""""
    )
    val p = docs.select(d => Fts.toTsVector(Param.named["lang", String], d.title)).compile
    assertEquals(p.fragment.sql, """SELECT to_tsvector($1::regconfig, "title") FROM "docs"""")
  }

  test("query builders render their functions") {
    val q = empty
      .select(_ =>
        (
          Fts.toTsQuery("fat & rat"),
          Fts.plainToTsQuery("english", "fat rats"),
          Fts.phraseToTsQuery("fat rats"),
          Fts.websearchToTsQuery("english", "\"fat rats\" -cat")
        )
      )
      .compile
    assertEquals(
      q.fragment.sql,
      """SELECT to_tsquery('fat & rat'), plainto_tsquery('english'::regconfig, 'fat rats'), phraseto_tsquery('fat rats'), websearch_to_tsquery('english'::regconfig, '"fat rats" -cat')"""
    )
  }

  test("tsquery combinators: andQuery / orQuery / negate / followedBy") {
    val a = Fts.toTsQuery("fat")
    val b = Fts.toTsQuery("rat")
    val q = empty.select(_ => (a.andQuery(b), a.orQuery(b), a.negate, a.followedBy(b))).compile
    assertEquals(
      q.fragment.sql,
      """SELECT (to_tsquery('fat') && to_tsquery('rat')), (to_tsquery('fat') || to_tsquery('rat')), (!! to_tsquery('fat')), (to_tsquery('fat') <-> to_tsquery('rat'))"""
    )
  }

  test("a ranked search with a headline, on a nullable generated tsvector column; named query param") {
    val search = docs
      .select(d =>
        (
          d.id,
          Fts.tsHeadline("english", d.body, Fts.websearchToTsQuery("english", Param.named["q", String])),
          Fts.tsRank(d.tsv, Fts.websearchToTsQuery("english", Param.named["q", String]))
        )
      )
      .where(d => d.tsv.matches(Fts.websearchToTsQuery("english", Param.named["q", String])))
      .orderBy(d => Fts.tsRank(d.tsv, Fts.websearchToTsQuery("english", Param.named["q", String])).desc)
      .limit(10)
      .compile
    val q = "websearch_to_tsquery('english'::regconfig, $%d)"
    assertEquals(
      search.fragment.sql,
      s"""SELECT "id", ts_headline('english'::regconfig, "body", ${q.format(1)}), ts_rank("tsv", ${q.format(
          2
        )}) FROM "docs" WHERE "tsv" @@ ${q.format(3)} ORDER BY ts_rank("tsv", ${q.format(4)}) DESC LIMIT 10"""
    )
    val af = search.bind((q = "fat rats"))
    assertEquals(af.fragment.encoder.encode(af.argument).flatten.map(_.value), List.fill(4)("fat rats"))
  }

  test("setweight and document concatenation") {
    val q = docs
      .select(d => Fts.setWeight(Fts.toTsVector(d.title), "A").concat(Fts.setWeight(Fts.toTsVector(d.body), "B")))
      .compile
    assertEquals(
      q.fragment.sql,
      """SELECT (setweight(to_tsvector("title"), 'A') || setweight(to_tsvector("body"), 'B')) FROM "docs""""
    )
  }

  test("tsRankCd renders ts_rank_cd") {
    val q = docs.select(d => Fts.tsRankCd(d.tsv, Fts.plainToTsQuery("rats"))).compile
    assertEquals(q.fragment.sql, """SELECT ts_rank_cd("tsv", plainto_tsquery('rats')) FROM "docs"""")
  }

  test("combinators are parenthesised, so they compose under @@ (equal precedence, left-associative)") {
    val q =
      docs.select(d => d.id).where(d => d.tsv.matches(Fts.toTsQuery("a").andQuery(Fts.toTsQuery("b").negate))).compile
    assertEquals(q.fragment.sql, """SELECT "id" FROM "docs" WHERE "tsv" @@ (to_tsquery('a') && (!! to_tsquery('b')))""")
  }
}
