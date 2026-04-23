package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object CteSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

class CteSuite extends PgFixture {
  import CteSuite.{Post, User}

  private val users = Table.of[User]("users").withDefault("created_at")
  private val posts = Table.of[Post]("posts").withDefault("created_at")

  private val pfx = "cte-it"

  test("simple CTE filters rows — outer SELECT sees only matching rows") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid1 = UUID.randomUUID
        val uid2 = UUID.randomUUID
        for {
          _ <- users.insert.values(NonEmptyList.of(
            (id = uid1, email = s"$pfx-a@x", age = 25, deleted_at = Option.empty[OffsetDateTime]),
            (id = uid2, email = s"$pfx-b@x", age = 15, deleted_at = Option.empty[OffsetDateTime])
          )).compile.run(s)
          // CTE: users with age >= 18
          young = cte("adults", users.select.where(u => u.age >= 18))
          rows <- young.select(u => u.email).where(u => u.email.like(s"$pfx-%")).compile.run(s)
          _ = assertEquals(rows, List(s"$pfx-a@x"), s"only adult should appear, got $rows")
        } yield ()
      }
    }
  }

  test("CTE joined with another table — produces cross-table result") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users.insert.values(
            (id = uid, email = s"$pfx-join@x", age = 30, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- posts.insert.values(
            (id = pid, user_id = uid, title = "Hello CTE")
          ).compile.run(s)
          active = cte("active_users", users.select.where(u => u.deleted_at.isNull))
          rows <- active
            .innerJoin(posts).on(r => r.active_users.id ==== r.posts.user_id)
            .select(r => (r.active_users.email, r.posts.title))
            .where(r => r.active_users.email === s"$pfx-join@x")
            .compile.run(s)
          _ = assertEquals(rows.map(_._2), List("Hello CTE"), s"got $rows")
        } yield ()
      }
    }
  }

  test("projected CTE — aggregate counts visible in outer query") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid1 = UUID.randomUUID
        val uid2 = UUID.randomUUID
        for {
          _ <- users.insert.values(NonEmptyList.of(
            (id = uid1, email = s"$pfx-agg1@x", age = 40, deleted_at = Option.empty[OffsetDateTime]),
            (id = uid2, email = s"$pfx-agg2@x", age = 40, deleted_at = Option.empty[OffsetDateTime])
          )).compile.run(s)
          _ <- posts.insert.values(NonEmptyList.of(
            (id = UUID.randomUUID, user_id = uid1, title = "p1"),
            (id = UUID.randomUUID, user_id = uid1, title = "p2"),
            (id = UUID.randomUUID, user_id = uid2, title = "p3")
          )).compile.run(s)
          // CTE: post counts per user
          counts = cte(
            "post_counts",
            posts.select(p => (p.user_id.as("uid"), Pg.count(p.id).as("cnt"))).groupBy(p => p.user_id)
          )
          rows <- counts.select(c => (c.uid, c.cnt)).compile.run(s)
          byUser = rows.toMap
          _      = assert(byUser.get(uid1).exists(_ >= 2L), s"uid1 should have ≥ 2 posts, got $byUser")
          _      = assert(byUser.get(uid2).exists(_ >= 1L), s"uid2 should have ≥ 1 post, got $byUser")
        } yield ()
      }
    }
  }

  test("chained CTEs — dependency evaluated in the right order") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- users.insert.values(NonEmptyList.of(
            (id = UUID.randomUUID, email = s"$pfx-ch1@x", age = 22, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"$pfx-ch2@x", age = 17, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = s"$pfx-ch3@x", age = 30, deleted_at = Option.empty[OffsetDateTime])
          )).compile.run(s)
          // base: users matching prefix
          base = cte("base_users", users.select.where(u => u.email.like(s"$pfx-ch%")))
          // derived: from base, only adults
          derived = cte("adults_only", base.select.where(u => u.age >= 18))
          rows <- derived.select(u => u.email).orderBy(u => u.email.asc).compile.run(s)
          _ = assertEquals(rows.toSet, Set(s"$pfx-ch1@x", s"$pfx-ch3@x"), s"got $rows")
        } yield ()
      }
    }
  }
}
