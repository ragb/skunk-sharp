package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object UpdateDeleteReturningSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class UpdateDeleteReturningSuite extends PgFixture {
  import UpdateDeleteReturningSuite.User

  private val users = Table.of[User]("users").withDefault("created_at")

  test("UPDATE ... RETURNING returns the new value") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("77777777-7777-7777-7777-777777777777")
        for {
          _ <- users.insert((
            id = id,
            email = "old@example.com",
            age = 20,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.update
              .set(u => u.email := "new@example.com")
              .where(u => u.id === id)
              .returning(u => u.email)
              .compile.unique(s),
            "new@example.com"
          )
        } yield ()
      }
    }
  }

  test("UPDATE ... RETURNING returningTuple returns multiple values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("88888888-8888-8888-8888-888888888888")
        for {
          _ <- users.insert((
            id = id,
            email = "tuple-ret@example.com",
            age = 10,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.update
              .set(u => u.age := 99)
              .where(u => u.id === id)
              .returningTuple(u => (u.id, u.age))
              .compile.unique(s),
            (id, 99)
          )
        } yield ()
      }
    }
  }

  test(".patch updates only the Some fields and leaves untouched fields unchanged") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.randomUUID
        for {
          _ <- users.insert((
            id = id,
            email = "patch-init@x",
            age = 20,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          // Patch the email; leave age alone.
          _ <- users.update
            .patch((email = Some("patch-new@x"), age = Option.empty[Int]))
            .where(u => u.id === id)
            .compile.run(s)
          row <- users.select.where(u => u.id === id).compile.unique(s)
          _ = assertEquals(row.email, "patch-new@x")
          _ = assertEquals(row.age, 20) // untouched
          _ = assertEquals(row.deleted_at, Option.empty[OffsetDateTime])
        } yield ()
      }
    }
  }

  test(".patch on a nullable column: Some(None) writes NULL, Some(Some(v)) writes v") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.randomUUID
        val ts = OffsetDateTime.parse("2024-06-01T00:00:00Z")
        val no = Option.empty[OffsetDateTime]
        for {
          _ <- users.insert((
            id = id,
            email = "patch-null@x",
            age = 30,
            created_at = OffsetDateTime.now(),
            deleted_at = Some(ts) // starts with a value set
          )).compile.run(s)
          // Set to NULL via Some(None)
          _ <- users.update
            .patch((deleted_at = Some(no)))
            .where(u => u.id === id)
            .compile.run(s)
          after1 <- users.select.where(u => u.id === id).compile.unique(s)
          _ = assertEquals(after1.deleted_at, no)
          // Now restore to a value via Some(Some(v))
          _ <- users.update
            .patch((deleted_at = Some(Some(ts))))
            .where(u => u.id === id)
            .compile.run(s)
          after2 <- users.select.where(u => u.id === id).compile.unique(s)
          _ = assertEquals(after2.deleted_at, Some(ts))
        } yield ()
      }
    }
  }

  test("DELETE ... RETURNING yields the deleted row values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("99999999-9999-9999-9999-999999999999")
        for {
          _ <- users.insert((
            id = id,
            email = "gone@example.com",
            age = 1,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.delete.where(u => u.id === id).returning(u => u.email).compile.unique(s),
            "gone@example.com"
          )
        } yield ()
      }
    }
  }
}
