package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object ProjectionsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class ProjectionsSuite extends PgFixture {
  import ProjectionsSuite.User

  private val users = Table.of[User]("users")

  test("SELECT with single-column .project returns a list of values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        for {
          _ <- users.insert((
            id = id,
            email = "proj-single@example.com",
            age = 21,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).run(s)
          emails <- users.select.where(u => u.id === id).apply(u => u.email).run(s)
          _ = assertEquals(emails, List("proj-single@example.com"))
        } yield ()
      }
    }
  }

  test("SELECT with .projectTuple returns a list of tuples") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        for {
          _ <- users.insert((
            id = id,
            email = "proj-tuple@example.com",
            age = 42,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).run(s)
          rows <- users.select.where(u => u.id === id).apply(u => (u.email, u.age)).run(s)
          _ = assertEquals(rows, List(("proj-tuple@example.com", 42)))
        } yield ()
      }
    }
  }

  test("Function calls work in projections: lower(email)") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
        for {
          _ <- users.insert((
            id = id,
            email = "UpperCase@Example.com",
            age = 33,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).run(s)
          lowered <- users.select.where(u => u.id === id).apply(u => Pg.lower(u.email)).run(s)
          _ = assertEquals(lowered, List("uppercase@example.com"))
        } yield ()
      }
    }
  }

}
