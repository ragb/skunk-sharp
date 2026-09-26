package skunk.sharp.tests

import cats.effect.IO
import cats.syntax.all.*
import skunk.sharp.dsl.*
import skunk.sharp.fts.*

object FtsSuite {
  case class Doc(id: Int, title: String, body: String, tsv: Option[TsVector])
}

/** Full-text search against Postgres's built-in `english` configuration. Matches V15__fts.sql. */
class FtsSuite extends PgFixture {
  import FtsSuite.*

  private val docs = Table.of[Doc]("fts_docs").withPrimary("id").withDefault("id").withGenerated("tsv")

  private val query = Fts.websearchToTsQuery("english", Param.named["q", String])

  // Compiled once; runs with (q = "…").
  private val search = docs
    .select(d => (d.title, Fts.tsRank(d.tsv, query), Fts.tsHeadline("english", d.body, query)))
    .where(d => d.tsv.matches(query))
    .orderBy(d => Fts.tsRank(d.tsv, query).desc)
    .limit(10)
    .compile

  test("ranked search: title matches (weight A) outrank body matches; headline marks the hits") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- List(
            "Rats in the kitchen" -> "The fat rat ate the cheese while the cat slept.",
            "Cat care"            -> "Cats sleep most of the day. A fat cat needs exercise.",
            "Cheese making"       -> "Aged cheese needs patience; rats are the enemy of every cheese cellar.",
            "Unrelated"           -> "Nothing about animals or dairy here."
          ).traverse_ { case (t, b) => docs.insert((title = t, body = b)).compile.run(s) }
          hits <- search.run(s)((q = "rats"))
          _ = assertEquals(hits.map(_._1), List("Rats in the kitchen", "Cheese making"))
          _ = assert(hits.head._2 > hits(1)._2, hits.toString)
          _ = assert(hits(1)._3.contains("<b>rats</b>"), hits(1)._3)
          // websearch syntax: a phrase and an exclusion; stemming makes "cats" match "cat".
          phrase <- search.run(s)((q = "\"fat cat\""))
          _ = assertEquals(phrase.map(_._1), List("Cat care"))
          noRats <- search.run(s)((q = "cheese -rats"))
          _ = assertEquals(noRats.map(_._1), Nil) // every cheese doc also mentions rats
          combo <- docs
            .select(d => d.title)
            .where(d => d.tsv.matches(Fts.toTsQuery("english", "cheese").andQuery(Fts.toTsQuery("english", "cellar"))))
            .compile
            .run(s)
          _ = assertEquals(combo, List("Cheese making"))
          negated <- docs
            .select(d => d.title)
            .where(d =>
              d.tsv.matches(Fts.toTsQuery("english", "fat").andQuery(Fts.toTsQuery("english", "cheese").negate))
            )
            .compile
            .run(s)
          _ = assertEquals(negated, List("Cat care"))
        } yield ()
      }
    }
  }

  test("SchemaValidator accepts the generated tsvector column") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, docs).map(r => assert(r.isValid, r.mismatches.map(_.pretty).mkString("; ")))
      }
    }
  }
}
