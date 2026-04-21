package skunk.sharp.tests

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
          _ <- users.insert.values(
            (
              id = aliceId,
              email = "alice@example.com",
              age = 30,
              created_at = now,
              deleted_at = Option.empty[OffsetDateTime]
            ),
            (
              id = bobId,
              email = "bob@example.com",
              age = 45,
              created_at = now,
              deleted_at = Option.empty[OffsetDateTime]
            )
          ).compile.run(s)
          _ <- assertIO(users.select.compile.run(s).map(_.size), 2)
          _ <- assertIO(users.select.where(u => u.age >= 18).compile.run(s).map(_.size), 2)
          _ <- users.update.set(u => u.age := 31).where(u => u.id === aliceId).compile.run(s)
          _ <- assertIO(users.select.where(u => u.id === aliceId).compile.run(s).map(_.map(_.age)), List(31))
          _ <- users.delete.where(u => u.id === bobId).compile.run(s)
          _ <- assertIO(users.select.compile.run(s).map(_.size), 1)
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
          _ <- users.insert.values(
            (id = a, email = "aaa@x", age = 30, created_at = now, deleted_at = Option.empty[OffsetDateTime]),
            (id = b, email = "bbb@x", age = 10, created_at = now, deleted_at = Option.empty[OffsetDateTime]),
            (id = c, email = "ccc@x", age = 20, created_at = now, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          _ <- assertIO(
            users.select.where(u => u.email.like("%@x")).orderBy(u => u.age.asc).apply(u => u.email).compile.run(s),
            List("bbb@x", "ccc@x", "aaa@x")
          )
          _ <- assertIO(
            users.select.where(u => u.email.like("%@x")).orderBy(u => u.age.desc).apply(u => u.email).compile.run(s),
            List("aaa@x", "ccc@x", "bbb@x")
          )
        } yield ()
      }
    }
  }

  test("BETWEEN / NOT BETWEEN round-trip") {
    withContainers { containers =>
      session(containers).use { s =>
        val no  = Option.empty[OffsetDateTime]
        val now = OffsetDateTime.now()
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "bt-u1@x", age = 10, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = "bt-u2@x", age = 30, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = "bt-u3@x", age = 80, created_at = now, deleted_at = no)
          ).compile.run(s)
          inBand <- users.select(u => u.email).where(u => u.age.between(18, 65))
            .where(u => u.email.like("bt-%@x")).compile.run(s).map(_.toSet)
          outOf <- users.select(u => u.email).where(u => u.age.notBetween(18, 65))
            .where(u => u.email.like("bt-%@x")).compile.run(s).map(_.toSet)
          _ = assertEquals(inBand, Set("bt-u2@x"))
          _ = assertEquals(outOf, Set("bt-u1@x", "bt-u3@x"))
        } yield ()
      }
    }
  }

  test("IS DISTINCT FROM on a nullable column — treats NULL vs value as distinct") {
    withContainers { containers =>
      session(containers).use { s =>
        val now = OffsetDateTime.now()
        val ts  = OffsetDateTime.parse("2020-01-01T00:00:00Z")
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "dist-a@x", age = 10, created_at = now, deleted_at = Some(ts)),
            (id = UUID.randomUUID, email = "dist-b@x", age = 20, created_at = now, deleted_at = None)
          ).compile.run(s)
          // Compare deleted_at against ts: row with None-deleted_at must still surface as DISTINCT from ts,
          // whereas row with Some(ts) must NOT (same value = not distinct).
          distinct <- users.select(u => u.email).where(u => u.deleted_at.isDistinctFrom(ts))
            .where(u => u.email.like("dist-%@x")).compile.run(s).map(_.toSet)
          _ = assertEquals(distinct, Set("dist-b@x"))
          eqSafe <- users.select(u => u.email).where(u => u.deleted_at.isNotDistinctFrom(ts))
            .where(u => u.email.like("dist-%@x")).compile.run(s).map(_.toSet)
          _ = assertEquals(eqSafe, Set("dist-a@x"))
        } yield ()
      }
    }
  }

  test("SIMILAR TO matches the regex-lite pattern") {
    withContainers { containers =>
      session(containers).use { s =>
        val no  = Option.empty[OffsetDateTime]
        val now = OffsetDateTime.now()
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "sim-alpha@x", age = 1, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = "sim-beta9@x", age = 2, created_at = now, deleted_at = no)
          ).compile.run(s)
          // Matches "sim-" + lowercase letters + "@x" — alpha passes, beta9 (has digit) fails.
          alphas <- users.select(u => u.email).where(u => u.email.similarTo("sim-[a-z]+@x"))
            .where(u => u.email.like("sim-%@x")).compile.run(s).map(_.toSet)
          _ = assertEquals(alphas, Set("sim-alpha@x"))
        } yield ()
      }
    }
  }

  test("searched CASE WHEN in a projection — buckets users by age") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "case-w"
        val no  = Option.empty[OffsetDateTime]
        val now = OffsetDateTime.now()
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = s"u1-$tag@x", age = 10, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = s"u2-$tag@x", age = 30, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = s"u3-$tag@x", age = 80, created_at = now, deleted_at = no)
          ).compile.run(s)
          rows <- users
            .select(u =>
              (
                u.email,
                caseWhen(u.age < 18, lit("minor"))
                  .when(u.age < 65, lit("adult"))
                  .otherwise(lit("senior"))
              )
            )
            .where(u => u.email.like(s"u%-$tag@x"))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(
            rows,
            Set[(String, String)](
              (s"u1-$tag@x", "minor"),
              (s"u2-$tag@x", "adult"),
              (s"u3-$tag@x", "senior")
            )
          )
        } yield ()
      }
    }
  }

  test("CASE WHEN in ORDER BY — custom sort priority by email prefix") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "case-o"
        val no  = Option.empty[OffsetDateTime]
        val now = OffsetDateTime.now()
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = s"b-$tag@x", age = 1, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = s"a-$tag@x", age = 2, created_at = now, deleted_at = no),
            (id = UUID.randomUUID, email = s"c-$tag@x", age = 3, created_at = now, deleted_at = no)
          ).compile.run(s)
          // Sort so that 'a-…' comes first, then 'b-…', then everything else.
          out <- users
            .select(u => u.email)
            .where(u => u.email.like(s"%-$tag@x"))
            .orderBy(u =>
              caseWhen(u.email === s"a-$tag@x", lit(0))
                .when(u.email === s"b-$tag@x", lit(1))
                .otherwise(lit(2))
                .asc
            )
            .compile.run(s)
          _ = assertEquals(out, List(s"a-$tag@x", s"b-$tag@x", s"c-$tag@x"))
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
          )).compile.run(s)
          _ <- assertIO(
            active.select.where(u => u.id === id).compile.run(s).map(_.map(_.email)),
            List("carol@example.com")
          )
        } yield ()
      }
    }
  }
}
