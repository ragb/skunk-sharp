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
          _ <- users
            .insert((id = uidA, email = "has-posts@x", age = 30, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- users
            .insert((id = uidB, email = "no-posts@x", age = 31, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = UUID.randomUUID, user_id = uidA, title = "hello")).compile.run(s)
          rows <- users
            .select(u => u.email)
            .where(u => u.id.in(posts.select(p => p.user_id)))
            .compile.run(s)
          _ = assertEquals(rows, List("has-posts@x"))
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
          _ <- users
            .insert((id = uidWith, email = "exw@x", age = 40, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- users
            .insert((id = uidWO, email = "exo@x", age = 41, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = UUID.randomUUID, user_id = uidWith, title = "ex")).compile.run(s)
          // Inner query built inside the outer lambda — references u.id (an outer column) by closure. The
          // outer alias makes u.id render as "u"."id" so Postgres correlates it to the outer source.
          rows <- users
            .alias("u")
            .select(u => u.email)
            .where(u => Pg.exists(posts.select(_ => TypedExpr.lit(1)).where(p => p.user_id ==== u.id)))
            .compile.run(s)
          _ = assertEquals(rows.toSet, Set("has-posts@x", "exw@x"))
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
          _    <- posts.insert((id = UUID.randomUUID, user_id = uid, title = "a")).compile.run(s)
          _    <- posts.insert((id = UUID.randomUUID, user_id = uid, title = "b")).compile.run(s)
          rows <- users
            .alias("u")
            .select(u =>
              (
                u.email,
                posts.select(_ => Pg.countAll).where(p => p.user_id ==== u.id).asExpr
              )
            )
            .where(u => u.id === uid)
            .compile.run(s)
          _ = assertEquals(rows, List(("scalar@x", 2L)))
        } yield ()
      }
    }
  }
}
