package skunk.sharp.tests

import cats.data.NonEmptyList
import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object InListSizesSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Dynamically-sized IN lists. Exercises the new `col.in(NonEmptyList[TypedExpr[T, Void]])` API across a range
 * of list sizes — both `Param.bind`-baked (the realistic runtime case) and `lit`-rendered (compile-time).
 *
 * The risk being verified: skunk's prepared-statement protocol historically struggles with variable-arity
 * `IN` lists because every `$N` placeholder needs an encoder slot. Our design has each `Param.bind(v)`
 * carry its own Void-args encoder (the value is baked via `contramap[Void](_ => v)`), so the visible
 * `Args` stays `Void` regardless of list size — execution should bind the right placeholders without
 * growing the user-facing tuple.
 */
class InListSizesSuite extends PgFixture {
  import InListSizesSuite.*

  private val users = Table.of[User]("users").withDefault("created_at")

  /** Insert N users with sequential emails, return their generated UUIDs in insert order. */
  private def seedUsers(s: skunk.Session[IO], n: Int, prefix: String): IO[List[UUID]] = {
    val ids = (1 to n).map(_ => UUID.randomUUID()).toList
    val rows = ids.zipWithIndex.map { case (id, i) =>
      (id = id, email = s"$prefix-$i@x", age = 20 + i, deleted_at = Option.empty[OffsetDateTime])
    }
    val insert = users.insert.values(NonEmptyList.fromListUnsafe(rows)).compile.run(s)
    insert.as(ids)
  }

  // -------- Runtime list, Param.bind path ---------------------------------------------------------

  List(1, 2, 5, 25, 100).foreach { n =>
    test(s"IN with $n Param.bind-baked values matches the right rows") {
      withContainers { containers =>
        session(containers).use { s =>
          val pfx = s"in-bind-$n"
          for {
            ids   <- seedUsers(s, n, pfx)
            // Pick a subset (every other id) so we can prove the IN really filtered.
            wanted = ids.zipWithIndex.collect { case (id, i) if i % 2 == 0 => id }
            nel    = NonEmptyList.fromListUnsafe(wanted.map(Param.bind(_)))
            rows  <- users.select(u => u.id).where(u => u.id.in(nel)).compile.run(s)
            _      = assertEquals(rows.toSet, wanted.toSet, s"n=$n returned wrong subset")
            _      = assertEquals(rows.size, wanted.size, s"n=$n returned wrong row count")
          } yield ()
        }
      }
    }
  }

  // -------- Compile-time literal path (lit) -------------------------------------------------------

  test("IN with `lit` literals (compile-time, rendered inline)") {
    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "in-lit"
        for {
          _    <- users.insert.values(
                    (id = UUID.randomUUID, email = s"$pfx-21@x", age = 21, deleted_at = Option.empty[OffsetDateTime]),
                    (id = UUID.randomUUID, email = s"$pfx-22@x", age = 22, deleted_at = Option.empty[OffsetDateTime]),
                    (id = UUID.randomUUID, email = s"$pfx-99@x", age = 99, deleted_at = Option.empty[OffsetDateTime])
                  ).compile.run(s)
          rows <- users
                    .select(u => u.email)
                    .where(u => u.email.like(Param.bind(s"$pfx-%")) && u.age.in(NonEmptyList.of(lit(21), lit(22))))
                    .compile.run(s)
          _ = assertEquals(rows.toSet, Set(s"$pfx-21@x", s"$pfx-22@x"))
        } yield ()
      }
    }
  }

  // -------- Single-element NonEmptyList — degenerate but legal -----------------------------------

  test("IN with a single Param.bind value (NonEmptyList of one)") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ids  <- seedUsers(s, 3, "in-one")
          one   = ids.head
          rows <- users
                    .select(u => u.id)
                    .where(u => u.id.in(NonEmptyList.one(Param.bind(one))))
                    .compile.run(s)
          _ = assertEquals(rows, List(one))
        } yield ()
      }
    }
  }

  // -------- Composing IN with other Param-bearing predicates --------------------------------------

  test("IN(Param.bind list) combined with WHERE Param[T] composes typed Args correctly") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ids  <- seedUsers(s, 6, "in-mix")
          want  = ids.take(3)
          q     = users
                    .select(u => u.id)
                    .where(u => u.id.in(NonEmptyList.fromListUnsafe(want.map(Param.bind(_)))))
                    .where(u => u.age >= Param[Int])
                    .compile
          // The Param.bind values bake into Void — only the `Param[Int]` shows up in the bind tuple.
          rows <- q.run(s)(0)
          _     = assertEquals(rows.toSet, want.toSet)
        } yield ()
      }
    }
  }

  // -------- Param.list[T](size) — single bind slot, fixed size ------------------------------------

  List(1, 2, 5, 25).foreach { n =>
    test(s"IN with Param.list[UUID]($n) — one bind slot, the list supplied at execute time") {
      withContainers { containers =>
        session(containers).use { s =>
          val pfx = s"in-paramlist-$n"
          for {
            ids   <- seedUsers(s, n, pfx)
            wanted = ids.zipWithIndex.collect { case (id, i) if i % 2 == 0 => id }
            // Build a query for exactly `wanted.size` ids — single prepared statement bound once.
            q      = users.select(u => u.id).where(u => u.id.in(Param.list[UUID](wanted.size))).compile
            _      = {
              val _: skunk.sharp.dsl.QueryTemplate[List[UUID], UUID] = q
            }
            rows  <- q.run(s)(wanted)
            _      = assertEquals(rows.toSet, wanted.toSet)
            _      = assertEquals(rows.size, wanted.size)
          } yield ()
        }
      }
    }
  }

  test("Param.list reuses one prepared statement across multiple bind calls of the same size") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          all <- seedUsers(s, 8, "in-paramlist-reuse")
          q    = users.select(u => u.id).where(u => u.id.in(Param.list[UUID](3))).compile
          // Three calls with three different lists of size 3 — same prepared statement.
          r1  <- q.run(s)(all.take(3))
          r2  <- q.run(s)(all.slice(2, 5))
          r3  <- q.run(s)(all.takeRight(3))
          _    = assertEquals(r1.toSet, all.take(3).toSet)
          _    = assertEquals(r2.toSet, all.slice(2, 5).toSet)
          _    = assertEquals(r3.toSet, all.takeRight(3).toSet)
        } yield ()
      }
    }
  }

  test("Param.list combined with WHERE Param[T] threads (List[T], T) at execute") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ids <- seedUsers(s, 6, "in-paramlist-mix")
          q    = users
                   .select(u => u.id)
                   .where(u => u.id.in(Param.list[UUID](3)))
                   .where(u => u.age >= Param[Int])
                   .compile
          _    = {
            val _: skunk.sharp.dsl.QueryTemplate[(List[UUID], Int), UUID] = q
          }
          rows <- q.run(s)((ids.take(3), 0))
          _     = assertEquals(rows.toSet, ids.take(3).toSet)
        } yield ()
      }
    }
  }

  // -------- Same predicate built dynamically and reused across sizes -----------------------------

  test("the same dynamic IN-builder works whether N=1 or N=64 (no plan-cache aliasing)") {
    def runFor(s: skunk.Session[IO], ids: List[UUID]): IO[Set[UUID]] =
      users
        .select(u => u.id)
        .where(u => u.id.in(NonEmptyList.fromListUnsafe(ids.map(Param.bind(_)))))
        .compile.run(s).map(_.toSet)

    withContainers { containers =>
      session(containers).use { s =>
        val pfx = "in-reuse"
        for {
          allIds <- seedUsers(s, 64, pfx)
          first   <- runFor(s,allIds.take(1))
          some    <- runFor(s,allIds.take(7))
          all     <- runFor(s,allIds)
          _ = assertEquals(first.size, 1)
          _ = assertEquals(some.size, 7)
          _ = assertEquals(all.size, 64)
        } yield ()
      }
    }
  }
}
