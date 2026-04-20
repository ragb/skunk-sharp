package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object SetOpSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Round-trip set operators against Postgres. Exercises [[SetOpQuery]] over real data — UNION, UNION ALL, INTERSECT,
 * EXCEPT — and nested chains, with one call to `.compile` at the very end of each builder.
 */
class SetOpSuite extends PgFixture {
  import SetOpSuite.*

  private val users = Table.of[User]("users").withDefault("created_at")

  /**
   * Fresh per-test dataset: every row's email carries the test tag so concurrent tests sharing one container can't
   * collide on the `users.email` UNIQUE index. The returned `Fixture` bundles the insert command, the three ids, and a
   * tag-scoped age filter to isolate queries from other tests' rows.
   */
  private case class Fixture(insert: CompiledCommand, old: UUID, mid: UUID, young: UUID, tag: String)

  private def fixtureFor(tag: String): Fixture = {
    val old    = UUID.randomUUID
    val mid    = UUID.randomUUID
    val young  = UUID.randomUUID
    val no     = Option.empty[OffsetDateTime]
    val insert = users.insert.values(
      (id = old, email = s"old-$tag@x", age = 70, deleted_at = no),
      (id = mid, email = s"mid-$tag@x", age = 40, deleted_at = no),
      (id = young, email = s"young-$tag@x", age = 20, deleted_at = no)
    ).compile
    Fixture(insert, old, mid, young, tag)
  }

  test("UNION deduplicates overlapping arms") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("union-dedup")
        for {
          _ <- f.insert.run(s)
          // Both arms match "mid-<tag>@x"; UNION should return it once. `like` filter narrows to this test's rows.
          result <- users.select(u => u.email).where(u => u.age >= 40 && u.email.like(s"%-${f.tag}@x"))
            .union(users.select(u => u.email).where(u => u.id === f.mid))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(result, Set(s"old-${f.tag}@x", s"mid-${f.tag}@x"))
        } yield ()
      }
    }
  }

  test("UNION ALL keeps duplicates") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("union-all")
        for {
          _      <- f.insert.run(s)
          result <- users.select(u => u.email).where(u => u.age >= 40 && u.email.like(s"%-${f.tag}@x"))
            .unionAll(users.select(u => u.email).where(u => u.id === f.mid))
            .compile.run(s)
          _ = assertEquals(result.count(_ == s"mid-${f.tag}@x"), 2)
          _ = assertEquals(result.toSet, Set(s"old-${f.tag}@x", s"mid-${f.tag}@x"))
        } yield ()
      }
    }
  }

  test("INTERSECT returns only rows present on both sides") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("intersect")
        for {
          _      <- f.insert.run(s)
          result <- users.select(u => u.email).where(u => u.age >= 30 && u.email.like(s"%-${f.tag}@x"))
            .intersect(users.select(u => u.email).where(u => u.age <= 50 && u.email.like(s"%-${f.tag}@x")))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(result, Set(s"mid-${f.tag}@x"))
        } yield ()
      }
    }
  }

  test("EXCEPT returns the left-only rows") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("except")
        for {
          _      <- f.insert.run(s)
          result <- users.select(u => u.email).where(u => u.age >= 30 && u.email.like(s"%-${f.tag}@x"))
            .except(users.select(u => u.email).where(u => u.age >= 50 && u.email.like(s"%-${f.tag}@x")))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(result, Set(s"mid-${f.tag}@x"))
        } yield ()
      }
    }
  }

  test("chained UNION .. EXCEPT resolves left-to-right") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("chain")
        for {
          _ <- f.insert.run(s)
          // UNION of (>=30) and (<=50) = all three; EXCEPT (==70) → two rows.
          result <- users.select(u => u.email).where(u => u.age >= 30 && u.email.like(s"%-${f.tag}@x"))
            .union(users.select(u => u.email).where(u => u.age <= 50 && u.email.like(s"%-${f.tag}@x")))
            .except(users.select(u => u.email).where(u => u.age === 70 && u.email.like(s"%-${f.tag}@x")))
            .compile.run(s).map(_.toSet)
          _ = assertEquals(result, Set(s"mid-${f.tag}@x", s"young-${f.tag}@x"))
        } yield ()
      }
    }
  }

  test("SetOpQuery used inside .in(...) works as a subquery") {
    withContainers { containers =>
      session(containers).use { s =>
        val f = fixtureFor("in-sub")
        for {
          _ <- f.insert.run(s)
          ids = users.select(u => u.id).where(u => u.age >= 60 && u.email.like(s"%-${f.tag}@x"))
            .union(users.select(u => u.id).where(u => u.age <= 30 && u.email.like(s"%-${f.tag}@x")))
          _ <- assertIO(
            users.select(u => u.email).where(u => u.id.in(ids)).compile.run(s).map(_.toSet),
            Set(s"old-${f.tag}@x", s"young-${f.tag}@x")
          )
        } yield ()
      }
    }
  }
}
