package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object InsertFromSelectSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Round-trip INSERT…SELECT against Postgres. Seeds a source table, promotes rows into the target via `.from(src)`, and
 * checks that parameters in the source's WHERE are applied and that row counts match.
 */
class InsertFromSelectSuite extends PgFixture {
  import InsertFromSelectSuite.*

  private val users = Table.of[User]("users").withPrimary("id").withUnique("email").withDefault("created_at")
  private val inbox = Table.of[User]("users_inbox").withPrimary("id").withUnique("email").withDefault("created_at")

  test("insert.from copies a filtered slice of the inbox into users") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "ifs-basic"
        val no  = Option.empty[OffsetDateTime]
        val u1  = UUID.randomUUID
        val u2  = UUID.randomUUID
        val u3  = UUID.randomUUID
        for {
          _ <- inbox.insert.values(
            (id = u1, email = s"young-$tag@x", age = 20, deleted_at = no),
            (id = u2, email = s"mid-$tag@x", age = 40, deleted_at = no),
            (id = u3, email = s"old-$tag@x", age = 70, deleted_at = no)
          ).compile.run(s)
          // Backfill: promote only adults from the inbox into the live users table.
          _ <- users.insert.from(inbox.select.where(u => u.age >= 18 && u.email.like(s"%-$tag@x"))).compile.run(s)
          _ <- assertIO(
            users.select(u => u.email).where(u => u.email.like(s"%-$tag@x")).compile.run(s).map(_.toSet),
            Set(s"young-$tag@x", s"mid-$tag@x", s"old-$tag@x")
          )
        } yield ()
      }
    }
  }

  test("insert.from with ON CONFLICT DO NOTHING — re-running the promotion is a no-op") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "ifs-conflict"
        val no  = Option.empty[OffsetDateTime]
        val u1  = UUID.randomUUID
        for {
          _ <- inbox.insert((id = u1, email = s"dupe-$tag@x", age = 25, deleted_at = no)).compile.run(s)
          // First promotion: the row lands in users.
          _ <- users.insert.from(inbox.select.where(u => u.email.like(s"%-$tag@x")))
            .onConflict(u => u.id)
            .doNothing
            .compile
            .run(s)
          // Second promotion: nothing changes — the ID conflict is silently skipped.
          _ <- users.insert.from(inbox.select.where(u => u.email.like(s"%-$tag@x")))
            .onConflict(u => u.id)
            .doNothing
            .compile
            .run(s)
          _ <- assertIO(
            users.select(u => u.email).where(u => u.email.like(s"%-$tag@x")).compile.run(s),
            List(s"dupe-$tag@x")
          )
        } yield ()
      }
    }
  }

  test("insert.from ... RETURNING streams the inserted rows' columns back") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "ifs-returning"
        val no  = Option.empty[OffsetDateTime]
        val u1  = UUID.randomUUID
        for {
          _   <- inbox.insert((id = u1, email = s"ret-$tag@x", age = 33, deleted_at = no)).compile.run(s)
          ids <- users.insert.from(inbox.select.where(u => u.email.like(s"%-$tag@x")))
            .returning(u => u.id)
            .compile.run(s)
          _ = assertEquals(ids, List(u1))
        } yield ()
      }
    }
  }
}
