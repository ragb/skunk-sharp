package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object SubquerySuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

class SubquerySuite extends PgFixture {
  import SubquerySuite.*

  private val users = Table.of[User]("users").withDefault("created_at")
  private val posts = Table.of[Post]("posts").withDefault("created_at")

  test("uncorrelated IN-subquery — users who have posts") {
    withContainers { containers =>
      session(containers).use { s =>
        val uidA = UUID.randomUUID
        val uidB = UUID.randomUUID
        for {
          _ <- users.insert.values(
            (id = uidA, email = "has-posts@x", age = 30, deleted_at = Option.empty[OffsetDateTime]),
            (id = uidB, email = "no-posts@x", age = 31, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- posts.insert((id = UUID.randomUUID, user_id = uidA, title = "hello")).compile.run(s)
          _ <- assertIO(
            users
              .select(u => u.email)
              .where(u => u.id.in(posts.select(p => p.user_id)))
              .compile.run(s),
            List("has-posts@x")
          )
        } yield ()
      }
    }
  }

  test("correlated EXISTS — users who have at least one post") {
    withContainers { containers =>
      session(containers).use { s =>
        val uidWith = UUID.randomUUID
        val uidWO   = UUID.randomUUID
        for {
          _ <- users.insert.values(
            (id = uidWith, email = "exw@x", age = 40, deleted_at = Option.empty[OffsetDateTime]),
            (id = uidWO, email = "exo@x", age = 41, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- posts.insert((id = UUID.randomUUID, user_id = uidWith, title = "ex")).compile.run(s)
          // Inner query built inside the outer lambda — references u.id (an outer column) by closure. The
          // outer alias makes u.id render as "u"."id" so Postgres correlates it to the outer source.
          _ <- assertIO(
            users
              .alias("u")
              .select(u => u.email)
              .where(u => Pg.exists(posts.select(_ => lit(1)).where(p => p.user_id ==== u.id)))
              .compile.run(s).map(_.toSet),
            Set("has-posts@x", "exw@x")
          )
        } yield ()
      }
    }
  }

  test("correlated NOT EXISTS — users with no posts") {
    withContainers { containers =>
      session(containers).use { s =>
        val uidWith = UUID.randomUUID
        val uidWO   = UUID.randomUUID
        for {
          _ <- users.insert.values(
            (id = uidWith, email = "nex-with@x", age = 50, deleted_at = Option.empty[OffsetDateTime]),
            (id = uidWO, email = "nex-without@x", age = 51, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- posts.insert((id = UUID.randomUUID, user_id = uidWith, title = "nex")).compile.run(s)
          _ <- assertIO(
            users
              .alias("u")
              .select(u => u.email)
              .where(u =>
                u.id.in(cats.data.NonEmptyList.of(uidWith, uidWO)) &&
                  Pg.notExists(posts.select(_ => lit(1)).where(p => p.user_id ==== u.id))
              )
              .compile.run(s),
            List("nex-without@x")
          )
        } yield ()
      }
    }
  }

  test("correlated scalar subquery in projection — email + per-user post count") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "scalar@x", age = 22, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert.values(
            (id = UUID.randomUUID, user_id = uid, title = "a"),
            (id = UUID.randomUUID, user_id = uid, title = "b")
          ).compile.run(s)
          _ <- assertIO(
            users
              .alias("u")
              .select(u =>
                (
                  u.email,
                  posts.select(_ => Pg.countAll).where(p => p.user_id ==== u.id).asExpr
                )
              )
              .where(u => u.id === uid)
              .compile.run(s),
            List(("scalar@x", 2L))
          )
        } yield ()
      }
    }
  }

  test("ANY over a subquery — age < ANY(ages of premium users)") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "any-sub"
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = s"a-$tag@x", age = 20, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"b-$tag@x", age = 30, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"c-$tag@x", age = 50, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"prem-1-$tag@x", age = 25, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"prem-2-$tag@x", age = 40, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          younger <- users
            .select(u => u.email)
            .where(u => u.age.ltAny(users.select(x => x.age).where(x => x.email.like(s"prem-%-$tag@x"))))
            .where(u => u.email.like(s"%-$tag@x"))
            .where(u => u.email.notSimilarTo(s"prem-%-$tag@x"))
            .compile.run(s).map(_.toSet)
          // anyone younger than at least one premium (premium ages = 25, 40) = 20, 30
          _ = assertEquals(younger, Set(s"a-$tag@x", s"b-$tag@x"))
        } yield ()
      }
    }
  }

  test("ALL over a subquery — age >= ALL(ages of a cohort) picks the oldest") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "all-sub"
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = s"u1-$tag@x", age = 10, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"u2-$tag@x", age = 20, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"u3-$tag@x", age = 30, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          oldest <- users
            .select(u => u.email)
            .where(u => u.age.gteAll(users.select(x => x.age).where(x => x.email.like(s"u%-$tag@x"))))
            .where(u => u.email.like(s"u%-$tag@x"))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(oldest, Set(s"u3-$tag@x"))
        } yield ()
      }
    }
  }

  test("DISTINCT ON returns one row per group — latest post per user") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "don"
        val uA  = UUID.randomUUID
        val uB  = UUID.randomUUID
        // Explicit timestamps so uA-latest is unambiguously newer than uA-first; `now()`-defaulted inserts can
        // collide within microsecond precision and make the DISTINCT ON pick non-deterministic.
        val t1 = OffsetDateTime.parse("2024-01-01T00:00:00Z")
        val t2 = OffsetDateTime.parse("2024-06-01T00:00:00Z")
        val t3 = OffsetDateTime.parse("2024-03-01T00:00:00Z")
        for {
          _ <- users.insert.values(
            (id = uA, email = s"uA-$tag@x", age = 20, deleted_at = Option.empty[OffsetDateTime]),
            (id = uB, email = s"uB-$tag@x", age = 21, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- posts.insert.values(
            (id = UUID.randomUUID, user_id = uA, title = s"uA-first-$tag", created_at = t1),
            (id = UUID.randomUUID, user_id = uA, title = s"uA-latest-$tag", created_at = t2),
            (id = UUID.randomUUID, user_id = uB, title = s"uB-only-$tag", created_at = t3)
          ).compile.run(s)
          rows <- posts
            .select
            .distinctOn(p => p.user_id)
            .where(p => p.title.like(s"u%-$tag"))
            .orderBy(p => (p.user_id.asc, p.created_at.desc))
            .apply(p => (p.user_id, p.title))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(rows, Set((uA, s"uA-latest-$tag"), (uB, s"uB-only-$tag")))
        } yield ()
      }
    }
  }

  test("Pg.overlaps — finds posts whose (created_at, created_at) overlaps a probe range") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag     = "overlaps"
        val ancient = OffsetDateTime.parse("2000-01-01T00:00:00Z")
        val fresh   = OffsetDateTime.parse("2024-06-01T00:00:00Z")
        val probeLo = OffsetDateTime.parse("2024-01-01T00:00:00Z")
        val probeHi = OffsetDateTime.parse("2024-12-31T00:00:00Z")
        val uid     = UUID.randomUUID
        for {
          _ <- users.insert((id = uid, email = s"ov-$tag@x", age = 20, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert.values(
            (id = UUID.randomUUID, user_id = uid, title = s"ancient-$tag", created_at = ancient),
            (id = UUID.randomUUID, user_id = uid, title = s"fresh-$tag", created_at = fresh)
          ).compile.run(s)
          hits <- posts.select(p => p.title)
            .where(p => Pg.overlaps(p.created_at, p.created_at, param(probeLo), param(probeHi)))
            .where(p => p.title.like(s"%-$tag"))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(hits, Set(s"fresh-$tag"))
        } yield ()
      }
    }
  }
}
