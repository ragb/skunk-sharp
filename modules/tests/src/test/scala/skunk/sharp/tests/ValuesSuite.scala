package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object ValuesSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Round-trip `Values.of(…)` against Postgres — both ways the unified shape shows up in SQL:
 *   - As the row source of `INSERT INTO … SELECT` (users have a literal set of rows to promote).
 *   - As a joinable [[Relation]] in `INNER JOIN` — the typical static-lookup table pattern.
 */
class ValuesSuite extends PgFixture {
  import ValuesSuite.*

  private val users = Table.of[User]("users").withPrimary("id").withUnique("email").withDefault("created_at")

  test("insert.from Values.of — literal rows land in the table") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "values-insert"
        val u1  = UUID.randomUUID
        val u2  = UUID.randomUUID
        val no  = Option.empty[OffsetDateTime]
        for {
          _ <- users.insert.from(
            Values.of(
              (id = u1, email = s"a-$tag@x", age = 22, deleted_at = no),
              (id = u2, email = s"b-$tag@x", age = 33, deleted_at = no)
            )
          ).compile.run(s)
          _ <- assertIO(
            users
              .select(u => u.email)
              .where(u => u.email.like(s"%-$tag@x"))
              .compile.run(s).map(_.toSet),
            Set(s"a-$tag@x", s"b-$tag@x")
          )
        } yield ()
      }
    }
  }

  test("INNER JOIN on a literal Values relation — label users by their age bucket") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "values-join"
        val u1  = UUID.randomUUID
        val u2  = UUID.randomUUID
        val no  = Option.empty[OffsetDateTime]
        for {
          _ <- users.insert.values(
            (id = u1, email = s"young-$tag@x", age = 22, deleted_at = no),
            (id = u2, email = s"old-$tag@x", age = 70, deleted_at = no)
          ).compile.run(s)
          // Literal (age, bucket) lookup. Not a table — just rows.
          buckets = Values.of(
            (age = 22, bucket = "young"),
            (age = 70, bucket = "senior")
          ).alias("buckets")
          labelled <- users
            .alias("u")
            .innerJoin(buckets)
            .on(r => r.u.age ==== r.buckets.age)
            .select(r => (r.u.email, r.buckets.bucket))
            .where(r => r.u.email.like(s"%-$tag@x"))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(
            labelled,
            Set((s"young-$tag@x", "young"), (s"old-$tag@x", "senior"))
          )
        } yield ()
      }
    }
  }
}
