package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object WindowFunctionSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class WindowFunctionSuite extends PgFixture {
  import WindowFunctionSuite.User

  private val users = Table.of[User]("users").withDefault("created_at")

  private def seed(s: skunk.Session[cats.effect.IO], pfx: String): cats.effect.IO[skunk.data.Completion] =
    users.insert.values(
      NonEmptyList.of(
        (id = UUID.randomUUID, email = s"$pfx-a@x", age = 10, deleted_at = Option.empty[OffsetDateTime]),
        (id = UUID.randomUUID, email = s"$pfx-b@x", age = 20, deleted_at = Option.empty[OffsetDateTime]),
        (id = UUID.randomUUID, email = s"$pfx-c@x", age = 30, deleted_at = Option.empty[OffsetDateTime]),
        (id = UUID.randomUUID, email = s"$pfx-d@x", age = 10, deleted_at = Option.empty[OffsetDateTime])
      )
    ).compile.run(s)

  test("row_number() assigns sequential 1-based numbers ordered by age") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-rn"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u => (u.email, Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc))))
            .where(u => u.email.like(s"$pfx-%"))
            .orderBy(u => u.age.asc)
            .compile.run(s)
          nums = rows.map(_._2)
          _    = assert(nums.nonEmpty, "expected results")
          _    = assert(nums.forall(_ > 0L), s"all row_numbers must be > 0, got $nums")
          _    = assertEquals(nums.toSet.size, nums.size, s"row_numbers must be unique, got $nums")
        } yield ()
      }
    }
  }

  test("sum(age) OVER (PARTITION BY age) equals age * count-in-partition") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-sum"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u => (u.age, Pg.sum(u.age).over(WindowSpec.partitionBy(u.age))))
            .where(u => u.email.like(s"$pfx-%"))
            .compile.run(s)
          // age=10 has 2 rows → partition sum = 20; age=30 has 1 → sum=30
          age10Rows = rows.filter(_._1 == 10)
          _         = assert(age10Rows.nonEmpty, "need age=10 rows")
          _         = assert(age10Rows.forall(_._2 == 20L), s"sum for age=10 partition should be 20, got $age10Rows")
          age30Rows = rows.filter(_._1 == 30)
          _         = assert(age30Rows.forall(_._2 == 30L), s"sum for age=30 partition should be 30, got $age30Rows")
        } yield ()
      }
    }
  }

  test("count(*) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) is cumulative") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-cnt"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u =>
              (
                u.age,
                Pg.countAll.over(
                  WindowSpec.orderBy(u.age.asc).rowsBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow)
                )
              )
            )
            .where(u => u.email.like(s"$pfx-%"))
            .orderBy(u => u.age.asc)
            .compile.run(s)
          _      = assert(rows.nonEmpty, "expected rows")
          counts = rows.map(_._2)
          paired = counts.zip(counts.drop(1))
          _      = assert(paired.forall { case (a, b) => b >= a }, s"cumulative counts must be non-decreasing: $counts")
          _      = assertEquals(counts.last, rows.size.toLong)
        } yield ()
      }
    }
  }

  test("lag(age) returns None for first row, previous value for subsequent rows") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-lag"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u => (u.age, Pg.lag(u.age).over(WindowSpec.orderBy(u.age.asc, u.email.asc))))
            .where(u => u.email.like(s"$pfx-%"))
            .orderBy(u => (u.age.asc, u.email.asc))
            .compile.run(s)
          _ = assert(rows.nonEmpty, "expected rows")
          _ = assertEquals(rows.head._2, None, s"first lag should be None, got ${rows.head._2}")
          _ = assert(rows.tail.exists(_._2.isDefined), s"later rows should have lag values: $rows")
        } yield ()
      }
    }
  }

  test("lag(age, 1, 0) returns 0 instead of NULL for the first row") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-lagd"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u => (u.age, Pg.lag(u.age, 1, 0).over(WindowSpec.orderBy(u.age.asc, u.email.asc))))
            .where(u => u.email.like(s"$pfx-%"))
            .orderBy(u => (u.age.asc, u.email.asc))
            .compile.run(s)
          _ = assert(rows.nonEmpty, "expected rows")
          _ = assertEquals(rows.head._2, 0, s"first lag with default should be 0, got ${rows.head._2}")
        } yield ()
      }
    }
  }

  test("rank() produces gaps for ties, dense_rank() does not") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-rnk"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u =>
              (u.age, Pg.rank.over(WindowSpec.orderBy(u.age.asc)), Pg.denseRank.over(WindowSpec.orderBy(u.age.asc)))
            )
            .where(u => u.email.like(s"$pfx-%"))
            .orderBy(u => u.age.asc)
            .compile.run(s)
          _ = assert(rows.nonEmpty, "expected rows")
          // age=10 appears twice → rank=1; age=20 → rank=3 (gap), dense_rank=2
          age20Rows                = rows.filter(_._1 == 20)
          _                        = assert(age20Rows.nonEmpty, "need age=20 rows")
          (_, rank20, denseRank20) = age20Rows.head
          _ = assert(rank20 >= 3L, s"rank for age=20 (after 2 age=10 ties) should be >= 3, got $rank20")
          _ = assert(denseRank20 == 2L, s"dense_rank for age=20 should be 2, got $denseRank20")
        } yield ()
      }
    }
  }

  test("first_value and last_value over unbounded frame") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "wf-fv"
        for {
          _    <- seed(s, pfx)
          rows <- users
            .select(u =>
              (
                u.age,
                Pg.firstValue(u.age).over(WindowSpec.orderBy(u.age.asc).rowsBetween(
                  FrameBound.UnboundedPreceding,
                  FrameBound.UnboundedFollowing
                )),
                Pg.lastValue(u.age).over(WindowSpec.orderBy(u.age.asc).rowsBetween(
                  FrameBound.UnboundedPreceding,
                  FrameBound.UnboundedFollowing
                ))
              )
            )
            .where(u => u.email.like(s"$pfx-%"))
            .compile.run(s)
          _ = assert(rows.nonEmpty, "expected rows")
          _ = assert(rows.forall(_._2 == 10), s"first_value should always be 10 (min age), got ${rows.map(_._2)}")
          _ = assert(rows.forall(_._3 == 30), s"last_value should always be 30 (max age), got ${rows.map(_._3)}")
        } yield ()
      }
    }
  }
}
