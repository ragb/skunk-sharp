package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object JoinSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

class JoinSuite extends PgFixture {
  import JoinSuite.*

  private val users = Table.of[User]("users").withDefault("created_at")
  private val posts = Table.of[Post]("posts").withDefault("created_at")

  test("INNER JOIN round-trips users + posts") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "join-u@x", age = 30, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _    <- posts.insert((id = pid, user_id = uid, title = "hello")).compile.run(s)
          rows <- users
            .alias("u")
            .innerJoin(posts.alias("p"))
            .on(r => r.u.id ==== r.p.user_id)
            .select(r => (r.u.email, r.p.title))
            .where(r => r.p.id === pid)
            .compile
            .run(s)
          _ = assertEquals(rows, List(("join-u@x", "hello")))
        } yield ()
      }
    }
  }

  test("LEFT JOIN returns NULL on the right side when no match") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "solo@x", age = 40, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          // No posts inserted for this user.
          rows <- users
            .alias("u")
            .leftJoin(posts.alias("p"))
            .on(r => r.u.id ==== r.p.user_id)
            .select(r => (r.u.email, r.p.title))
            .where(r => r.u.id === uid)
            .compile
            .run(s)
          _ = assertEquals(rows, List(("solo@x", Option.empty[String])))
        } yield ()
      }
    }
  }

  test("JOIN + GROUP BY + COUNT aliased: posts per user") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "many@x", age = 28, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _    <- posts.insert((id = UUID.randomUUID, user_id = uid, title = "a")).compile.run(s)
          _    <- posts.insert((id = UUID.randomUUID, user_id = uid, title = "b")).compile.run(s)
          _    <- posts.insert((id = UUID.randomUUID, user_id = uid, title = "c")).compile.run(s)
          rows <- users
            .alias("u")
            .leftJoin(posts.alias("p"))
            .on(r => r.u.id ==== r.p.user_id)
            .select(r => (r.u.email, Pg.count(r.p.id).as("n")))
            .where(r => r.u.id === uid)
            .groupBy(r => r.u.email)
            .compile
            .run(s)
          _ = assertEquals(rows, List(("many@x", 3L)))
        } yield ()
      }
    }
  }
}
