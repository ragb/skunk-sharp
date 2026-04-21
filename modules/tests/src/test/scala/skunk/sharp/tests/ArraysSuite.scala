package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.data.Arr
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.pg.ArrayOps.*

object ArraysSuite {
  case class ArrayPost(id: Int, tags: Arr[String], score: Int)
}

class ArraysSuite extends PgFixture {
  import ArraysSuite.*

  private val posts = Table.of[ArrayPost]("array_posts").withPrimary("id")

  test("round-trip: insert + select an Arr[String] column") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 101, tags = Arr("scala", "pg"), score = 10),
            (id = 102, tags = Arr("scala"), score = 20),
            (id = 103, tags = Arr("sql"), score = 30)
          ).compile.run(s)
          rows <- posts.select.where(p => p.id.in(NonEmptyList.of(101, 102, 103))).compile.run(s)
          _      = assertEquals(rows.map(_.id).toSet, Set(101, 102, 103))
          tag101 = rows.find(_.id == 101).get.tags.flattenTo(List)
          _      = assertEquals(tag101, List("scala", "pg"))
        } yield ()
      }
    }
  }

  test("@> contains — find posts whose tags contain 'scala'") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 201, tags = Arr("scala", "pg"), score = 1),
            (id = 202, tags = Arr("python"), score = 2)
          ).compile.run(s)
          ids <- posts
            .select(p => p.id)
            .where(p => p.tags.contains(param(Arr("scala"))))
            .where(p => p.id.in(NonEmptyList.of(201, 202)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(201))
        } yield ()
      }
    }
  }

  test("<@ containedBy — filter tags subset of a probe") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 301, tags = Arr("a"), score = 1),
            (id = 302, tags = Arr("a", "b"), score = 2),
            (id = 303, tags = Arr("a", "b", "c"), score = 3)
          ).compile.run(s)
          ids <- posts
            .select(p => p.id)
            .where(p => p.tags.containedBy(param(Arr("a", "b"))))
            .where(p => p.id.in(NonEmptyList.of(301, 302, 303)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(301, 302))
        } yield ()
      }
    }
  }

  test("&& overlaps — rows sharing at least one tag with the probe") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 401, tags = Arr("x", "y"), score = 1),
            (id = 402, tags = Arr("z"), score = 2),
            (id = 403, tags = Arr("y", "w"), score = 3)
          ).compile.run(s)
          ids <- posts
            .select(p => p.id)
            .where(p => p.tags.overlaps(param(Arr("y"))))
            .where(p => p.id.in(NonEmptyList.of(401, 402, 403)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(401, 403))
        } yield ()
      }
    }
  }

  test("= ANY(array) via .elemOf — scalar membership in an array column") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 501, tags = Arr("alpha"), score = 1),
            (id = 502, tags = Arr("beta"), score = 2),
            (id = 503, tags = Arr("gamma"), score = 3)
          ).compile.run(s)
          ids <- posts
            .select(p => p.id)
            .where(p => param("alpha").elemOf(p.tags))
            .where(p => p.id.in(NonEmptyList.of(501, 502, 503)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(501))
        } yield ()
      }
    }
  }

  test("array_length / cardinality round-trip") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 601, tags = Arr("a", "b", "c"), score = 1),
            (id = 602, tags = Arr("a", "b"), score = 2)
          ).compile.run(s)
          lens <- posts
            .select(p => (p.id, Pg.arrayLength(p.tags), Pg.cardinality(p.tags)))
            .where(p => p.id.in(NonEmptyList.of(601, 602)))
            .compile.run(s).map(rs => rs.map { case (id, len, card) => id -> (len, card) }.toMap)
          _ = assertEquals(lens(601), (Option(3), 3))
          _ = assertEquals(lens(602), (Option(2), 2))
        } yield ()
      }
    }
  }

  test("array_agg aggregates rows into a single array") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 611, tags = Arr("x"), score = 1),
            (id = 612, tags = Arr("y"), score = 2)
          ).compile.run(s)
          agg <- posts
            .select(p => Pg.arrayAgg(p.id))
            .where(p => p.id.in(NonEmptyList.of(611, 612)))
            .compile
            .unique(s)
          _ = assertEquals(agg.flattenTo(List).toSet, Set(611, 612))
        } yield ()
      }
    }
  }

  test("Pg.unnestAsRelation — LATERAL-expand an Arr[String] column into rows") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- posts.insert.values(
            (id = 801, tags = Arr("a", "b", "c"), score = 1),
            (id = 802, tags = Arr("d"), score = 2)
          ).compile.run(s)
          rows <- posts
            .alias("p")
            .innerJoinLateral(p => Pg.unnestAsRelation(p.tags).alias("t"))
            .on(_ => lit(true))
            .select(r => (r.p.id, r.t.v))
            .where(r => r.p.id.in(NonEmptyList.of(801, 802)))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(
            rows,
            Set[(Int, String)](
              (801, "a"),
              (801, "b"),
              (801, "c"),
              (802, "d")
            )
          )
        } yield ()
      }
    }
  }

  test("Pg.generateSeries — CROSS JOIN an int range with a base table") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _    <- posts.insert((id = 901, tags = Arr("x"), score = 1)).compile.run(s)
          rows <- posts
            .alias("p")
            .crossJoin(Pg.generateSeries(1, 3).alias("g"))
            .select(r => (r.p.id, r.g.n))
            .where(r => r.p.id === 901)
            .compile.run(s).map(_.toSet)
          _ = assertEquals(rows, Set[(Int, Int)]((901, 1), (901, 2), (901, 3)))
        } yield ()
      }
    }
  }

  test("generic collPgTypeFor — Vector[String] column round-trips") {
    withContainers { containers =>
      session(containers).use { s =>
        case class VecPost(id: Int, tags: Vector[String], score: Int)
        val vecPosts = Table.of[VecPost]("array_posts").withPrimary("id")
        for {
          _   <- vecPosts.insert((id = 701, tags = Vector("v1", "v2"), score = 7)).compile.run(s)
          row <- vecPosts.select.where(p => p.id === 701).compile.unique(s)
          _ = assertEquals(row.tags, Vector("v1", "v2"))
        } yield ()
      }
    }
  }
}
