package skunk.sharp.tests

import cats.data.NonEmptyList
import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime

object SubsetInsertSuite {
  case class Event(id: Long, kind: String, payload: String, created_at: OffsetDateTime)

  case class NewEvent(kind: String, payload: String)
}

class SubsetInsertSuite extends PgFixture {
  import SubsetInsertSuite.Event

  private val events =
    Table.of[Event]("events").withDefault("id").withDefault("created_at")

  test("insert can omit a sequence PK and a DEFAULT-now() column; DB fills them in") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          r <- events
            .insert((kind = "sign-in", payload = "user=alice"))
            .returningTuple(e => (e.id, e.kind, e.payload, e.created_at))
            .unique(s)
          (id, kind, payload, createdAt) = r
          _                              = assert(id > 0, s"sequence PK was filled in, got $id")
          _                              = assertEquals(kind, "sign-in")
          _                              = assertEquals(payload, "user=alice")
          _                              = assert(createdAt != null, "created_at was filled in by DEFAULT now()")
        } yield ()
      }
    }
  }

  test("insert accepts a case-class subset and the DB fills defaulted columns") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          row <- events
            .insert(SubsetInsertSuite.NewEvent(kind = "case-class", payload = "works"))
            .returningTuple(e => (e.id, e.kind, e.payload))
            .unique(s)
          (id, kind, payload) = row
          _                   = assert(id > 0, s"sequence PK filled in for case-class insert, got $id")
          _                   = assertEquals(kind, "case-class")
          _                   = assertEquals(payload, "works")
        } yield ()
      }
    }
  }

  test("insert.values with a NonEmptyList batches rows and omits defaulted columns") {
    withContainers { containers =>
      session(containers).use { s =>
        val rows = NonEmptyList.of(
          (kind = "a", payload = "one"),
          (kind = "b", payload = "two"),
          (kind = "c", payload = "three")
        )
        for {
          inserted <- events.insert.values(rows).returning(e => e.kind).run(s)
          _ = assertEquals(inserted, List("a", "b", "c"))
        } yield ()
      }
    }
  }
}
