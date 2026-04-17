package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object DslRoundTripSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class ActiveUser(id: UUID, email: String, age: Int)
}

class DslRoundTripSuite extends PgFixture {
  import DslRoundTripSuite.*

  private val users  = Table.of[User]("users").withPrimary("id").withUnique("email").withDefault("created_at")
  private val active = View.of[ActiveUser]("active_users")

  test("insert, select, update, delete round-trip") {
    withContainers { containers =>
      session(containers).use { s =>
        val aliceId = UUID.fromString("11111111-1111-1111-1111-111111111111")
        val bobId   = UUID.fromString("22222222-2222-2222-2222-222222222222")
        val now     = OffsetDateTime.now()
        for {
          _ <- users.insert((
            id = aliceId,
            email = "alice@example.com",
            age = 30,
            created_at = now,
            deleted_at = None
          )).run(s)
          _ <-
            users.insert((id = bobId, email = "bob@example.com", age = 45, created_at = now, deleted_at = None)).run(s)
          all <- users.select.run(s)
          _ = assertEquals(all.size, 2)
          adult <- users.select.where(u => u.age >= 18).run(s)
          _ = assertEquals(adult.size, 2)
          _     <- users.update.set(u => u.age := 31).where(u => u.id === aliceId).run(s)
          alice <- users.select.where(u => u.id === aliceId).run(s)
          _ = assertEquals(alice.map(_.age), List(31))
          _      <- users.delete.where(u => u.id === bobId).run(s)
          final_ <- users.select.run(s)
          _ = assertEquals(final_.size, 1)
        } yield ()
      }
    }
  }

  test("ORDER BY actually sorts rows against the database") {
    withContainers { containers =>
      session(containers).use { s =>
        val a   = UUID.fromString("40000000-0000-0000-0000-000000000001")
        val b   = UUID.fromString("40000000-0000-0000-0000-000000000002")
        val c   = UUID.fromString("40000000-0000-0000-0000-000000000003")
        val now = OffsetDateTime.now()
        for {
          _   <- users.insert((id = a, email = "aaa@x", age = 30, created_at = now, deleted_at = None)).run(s)
          _   <- users.insert((id = b, email = "bbb@x", age = 10, created_at = now, deleted_at = None)).run(s)
          _   <- users.insert((id = c, email = "ccc@x", age = 20, created_at = now, deleted_at = None)).run(s)
          asc <- users.select.where(u => u.email.like("%@x")).orderBy(u => u.age.asc).apply(u => u.email).run(s)
          _ = assertEquals(asc, List("bbb@x", "ccc@x", "aaa@x"))
          desc <- users.select.where(u => u.email.like("%@x")).orderBy(u => u.age.desc).apply(u => u.email).run(s)
          _ = assertEquals(desc, List("aaa@x", "ccc@x", "bbb@x"))
        } yield ()
      }
    }
  }

  test("select against a view works; mutations would not compile") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("33333333-3333-3333-3333-333333333333")
        for {
          _ <- users.insert((
            id = id,
            email = "carol@example.com",
            age = 25,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).run(s)
          rows <- active.select.where(u => u.id === id).run(s)
          _ = assertEquals(rows.map(_.email), List("carol@example.com"))
        } yield ()
      }
    }
  }
}
