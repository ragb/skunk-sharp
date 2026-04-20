package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object GroupBySuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class GroupBySuite extends PgFixture {
  import GroupBySuite.User

  private val users = Table.of[User]("users").withDefault("created_at")

  test("aggregate count(*) and count(col) return the row count") {
    withContainers { containers =>
      session(containers).use { s =>
        val rows = NonEmptyList.of(
          (id = UUID.randomUUID, email = "g1@x", age = 20, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "g2@x", age = 25, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "g3@x", age = 30, deleted_at = Option.empty[OffsetDateTime])
        )
        for {
          _     <- users.insert.values(rows).compile.run(s)
          total <- users.select(_ => Pg.countAll).compile.unique(s)
          _ = assert(total >= 3L, s"expected at least 3 rows, got $total")
          _ <- assertIO(users.select(u => Pg.count(u.age)).compile.unique(s), total)
        } yield ()
      }
    }
  }

  test("GROUP BY age with count + having") {
    withContainers { containers =>
      session(containers).use { s =>
        val seed = NonEmptyList.of(
          (id = UUID.randomUUID, email = "gb1@x", age = 21, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "gb2@x", age = 21, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "gb3@x", age = 22, deleted_at = Option.empty[OffsetDateTime])
        )
        for {
          _    <- users.insert.values(seed).compile.run(s)
          rows <- users
            .select(u => (u.age, Pg.count(u.id)))
            .groupBy(u => u.age)
            .having(u => Pg.count(u.id) >= 1L)
            .compile
            .run(s)
          byAge = rows.toMap
          _     = assert(byAge(21) >= 2L, s"age=21 should have at least 2, got ${byAge.get(21)}")
          _     = assert(byAge(22) >= 1L, s"age=22 should have at least 1, got ${byAge.get(22)}")
        } yield ()
      }
    }
  }

  test("sum / avg / min / max on age (sum(int) → bigint, avg(int) → numeric)") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "m1@x", age = 10, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = "m2@x", age = 30, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          stats <- users
            .select(u => (Pg.sum(u.age), Pg.avg(u.age), Pg.min(u.age), Pg.max(u.age)))
            .compile
            .unique(s)
          (sum, avg, min, max) = stats
          _                    = assert(sum >= 40L, s"sum should include both inserted rows, got $sum")
          _                    = assert(avg >= BigDecimal(0), s"avg should be non-negative, got $avg")
          _                    = assert(min <= 10, s"min should be <= 10, got $min")
          _                    = assert(max >= 30, s"max should be >= 30, got $max")
        } yield ()
      }
    }
  }

  test("aliased aggregate round-trips: SELECT count(*) AS \"cnt\" FROM users") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          cnt <- users.select(_ => Pg.countAll.as("cnt")).compile.unique(s)
          _ = assert(cnt >= 0L, s"aliased count should return a non-negative value, got $cnt")
        } yield ()
      }
    }
  }

  test("aliased column + aliased aggregate with GROUP BY: SELECT age AS a, count(id) AS n") {
    withContainers { containers =>
      session(containers).use { s =>
        val seed = NonEmptyList.of(
          (id = UUID.randomUUID, email = "al1@x", age = 41, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "al2@x", age = 41, deleted_at = Option.empty[OffsetDateTime]),
          (id = UUID.randomUUID, email = "al3@x", age = 42, deleted_at = Option.empty[OffsetDateTime])
        )
        for {
          _    <- users.insert.values(seed).compile.run(s)
          rows <- users
            .select(u => (u.age.as("a"), Pg.count(u.id).as("n")))
            .groupBy(u => u.age)
            .having(u => Pg.count(u.id) >= 1L)
            .compile
            .run(s)
          byAge = rows.toMap
          _     = assert(byAge(41) >= 2L, s"age=41 should have at least 2, got ${byAge.get(41)}")
          _     = assert(byAge(42) >= 1L, s"age=42 should have at least 1, got ${byAge.get(42)}")
        } yield ()
      }
    }
  }

  test("countDistinct — deduplicates across input rows") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "cd1@x", age = 99, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = "cd2@x", age = 99, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = "cd3@x", age = 100, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          n <- users.select(u => Pg.countDistinct(u.age)).where(u => u.age >= 99).compile.unique(s)
          _ = assert(n >= 2L, s"distinct ages ≥ 99 should be ≥ 2, got $n")
        } yield ()
      }
    }
  }

  test("stringAgg concatenates emails per age-bucket using a separator") {
    withContainers { containers =>
      session(containers).use { s =>
        val bucket = 77
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "sa1@x", age = bucket, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = "sa2@x", age = bucket, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          concat <- users.select(u => Pg.stringAgg(u.email, ", ")).where(u => u.age === bucket).compile.unique(s)
          _ = assert(concat.contains("sa1@x") && concat.contains("sa2@x"), s"got '$concat'")
          _ = assert(concat.contains(", "), s"separator missing: '$concat'")
        } yield ()
      }
    }
  }

  test("boolAnd / boolOr over a predicate expression") {
    withContainers { containers =>
      session(containers).use { s =>
        val bucket = 55
        for {
          _ <- users.insert.values(
            (id = UUID.randomUUID, email = "ba1@x", age = bucket, deleted_at = Option.empty[OffsetDateTime]),
            (id = UUID.randomUUID, email = "ba2@x", age = bucket, deleted_at = Option.empty[OffsetDateTime])
          ).compile.run(s)
          // All rows in this bucket have age >= 10 — boolAnd should be true.
          _ <- assertIO(
            users
              .select(u => Pg.boolAnd(u.age >= 10))
              .where(u => u.age === bucket)
              .compile.unique(s),
            true
          )
          // Not all are ≥ 100 — boolAnd should be false, boolOr should also be false.
          _ <- assertIO(
            users
              .select(u => Pg.boolAnd(u.age >= 100))
              .where(u => u.age === bucket)
              .compile.unique(s),
            false
          )
          _ <- assertIO(
            users
              .select(u => Pg.boolOr(u.age >= 100))
              .where(u => u.age === bucket)
              .compile.unique(s),
            false
          )
        } yield ()
      }
    }
  }
}
