package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object UpdateFromDeleteUsingSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
  case class Tag(id: UUID, post_id: UUID, name: String)
}

/**
 * Integration tests for `UPDATE … FROM` and `DELETE … USING`. Uses the existing `users`, `posts`, and `tags` tables
 * from the shared migrations so the only test data lives in these tests.
 */
class UpdateFromDeleteUsingSuite extends PgFixture {
  import UpdateFromDeleteUsingSuite.*

  private val users =
    Table.of[User]("users").withPrimary("id").withUnique("email").withDefault("created_at")

  private val posts = Table.of[Post]("posts").withPrimary("id").withDefault("created_at")
  private val tags  = Table.of[Tag]("tags").withPrimary("id")

  test("UPDATE … FROM: set a column using a value from a joined source") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "upd-from"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          // Seed: one user at age 10, one post belonging to that user
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 10,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = "hello", created_at = now)).compile.run(s)
          // UPDATE users SET age = 99 FROM posts WHERE users.id = posts.user_id AND posts.id = <postId>
          _ <- users.update
            .from(posts)
            .set(r => r.users.age := 99)
            .where(r => r.users.id ==== r.posts.user_id && r.posts.id === postId)
            .compile.run(s)
          // Verify age changed
          _ <- assertIO(
            users.select(u => u.age).where(u => u.id === userId).compile.run(s),
            List(99)
          )
        } yield ()
      }
    }
  }

  test("UPDATE … FROM: column from FROM source used on RHS of SET") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "upd-from-rhs"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag-old@example.com",
            age = 1,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = s"$tag-title", created_at = now)).compile.run(s)
          // UPDATE users SET email = posts.title FROM posts WHERE users.id = posts.user_id
          _ <- users.update
            .from(posts)
            .set(r => r.users.email := r.posts.title)
            .where(r => r.users.id ==== r.posts.user_id && r.posts.id === postId)
            .compile.run(s)
          _ <- assertIO(
            users.select(u => u.email).where(u => u.id === userId).compile.run(s),
            List(s"$tag-title")
          )
        } yield ()
      }
    }
  }

  test("UPDATE … FROM with two extra sources") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "upd-from-2src"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val tagId  = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 1,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = "t", created_at = now)).compile.run(s)
          _ <- tags.insert((id = tagId, post_id = postId, name = "scala")).compile.run(s)
          // UPDATE users SET email = tags.name FROM posts, tags
          //   WHERE users.id = posts.user_id AND posts.id = tags.post_id AND tags.id = <tagId>
          _ <- users.update
            .from(posts)
            .from(tags)
            .set(r => r.users.email := r.tags.name)
            .where(r =>
              r.users.id ==== r.posts.user_id && r.posts.id ==== r.tags.post_id && r.tags.id === tagId
            )
            .compile.run(s)
          _ <- assertIO(
            users.select(u => u.email).where(u => u.id === userId).compile.run(s),
            List("scala")
          )
        } yield ()
      }
    }
  }

  test("UPDATE … FROM RETURNING returns updated rows") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "upd-from-ret"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 5,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _      <- posts.insert((id = postId, user_id = userId, title = "t", created_at = now)).compile.run(s)
          emails <- users.update
            .from(posts)
            .set(r => r.users.age := 42)
            .where(r => r.users.id ==== r.posts.user_id && r.posts.id === postId)
            .returning(r => r.users.email)
            .compile.run(s)
          _ = assertEquals(emails, List(s"$tag@example.com"))
        } yield ()
      }
    }
  }

  test("DELETE … USING: delete rows matched via a joined source") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "del-using"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 1,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = "del-me", created_at = now)).compile.run(s)
          // DELETE FROM posts USING users WHERE posts.user_id = users.id AND users.id = <userId>
          // (delete child rows based on parent info — avoids FK violation)
          _ <- posts.delete
            .using(users)
            .where(r => r.posts.user_id ==== r.users.id && r.users.id === userId)
            .compile.run(s)
          _ <- assertIO(
            posts.select(p => p.id).where(p => p.id === postId).compile.run(s),
            List.empty[UUID]
          )
        } yield ()
      }
    }
  }

  test("DELETE … USING two sources: delete tags via posts+users chain") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "del-using-2src"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val tagId  = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 1,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = "p", created_at = now)).compile.run(s)
          _ <- tags.insert((id = tagId, post_id = postId, name = "x")).compile.run(s)
          // DELETE FROM tags USING posts, users WHERE tags.post_id = posts.id AND posts.user_id = users.id AND users.id = <userId>
          _ <- tags.delete
            .using(posts)
            .using(users)
            .where(r =>
              r.tags.post_id ==== r.posts.id && r.posts.user_id ==== r.users.id && r.users.id === userId
            )
            .compile.run(s)
          _ <- assertIO(
            tags.select(t => t.id).where(t => t.id === tagId).compile.run(s),
            List.empty[UUID]
          )
        } yield ()
      }
    }
  }

  test("DELETE … USING RETURNING returns deleted rows") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag    = "del-using-ret"
        val userId = UUID.randomUUID()
        val postId = UUID.randomUUID()
        val now    = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = userId,
            email = s"$tag@example.com",
            age = 1,
            created_at = now,
            deleted_at = None
          )).compile.run(s)
          _ <- posts.insert((id = postId, user_id = userId, title = "ret-title", created_at = now)).compile.run(s)
          // DELETE FROM posts USING users RETURNING posts.title
          titles <- posts.delete
            .using(users)
            .where(r => r.posts.user_id ==== r.users.id && r.users.id === userId)
            .returning(r => r.posts.title)
            .compile.run(s)
          _ = assertEquals(titles, List("ret-title"))
        } yield ()
      }
    }
  }

}
