package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.LocalDate

object PartitionedTableSuite {
  case class Event(id: Long, day: LocalDate, kind: String, payload: String)
}

/**
 * Partitioned tables need no special DSL support: the parent is a plain `Table`, Postgres routes rows, and a single
 * partition is the same declaration under another name (`.renamed`). Matches V13__partitioned_events.sql.
 */
class PartitionedTableSuite extends PgFixture {
  import PartitionedTableSuite.*

  private val events = Table.of[Event]("partitioned_events").withCompositePrimary[("id", "day")].withDefault("id")

  private def month(m: Int): String = f"partitioned_events_2026_$m%02d"

  test("the parent and a renamed partition both validate against the schema") {
    val febName = month(2) // a runtime-computed name works once bound to a stable val
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, events, events.renamed("partitioned_events_2026_01"), events.renamed(febName))
          .map(r => assert(r.isValid, r.mismatches.map(_.pretty).mkString("; ")))
      }
    }
  }

  test("rows inserted through the parent land in, and move between, partitions") {
    val jan = events.renamed("partitioned_events_2026_01")
    val feb = events.renamed("partitioned_events_2026_02")
    withContainers { containers =>
      session(containers).use { s =>
        for {
          id <- events
            .insert((day = LocalDate.of(2026, 1, 5), kind = "a", payload = "x"))
            .returning(e => e.id)
            .compile
            .unique(s)
          inJan <- jan.select(e => e.id).where(e => e.id === Param.bind(id)).compile.option(s)
          _ = assertEquals(inJan, Some(id))
          // Updating the partition key moves the row to the matching partition.
          _ <- events.update
            .set(e => e.day := Param.bind(LocalDate.of(2026, 2, 10)))
            .where(e => e.id === Param.bind(id))
            .compile
            .run(s)
          stillJan <- jan.select(e => e.id).where(e => e.id === Param.bind(id)).compile.option(s)
          nowFeb   <- feb.select(e => e.id).where(e => e.id === Param.bind(id)).compile.option(s)
          _ = assertEquals(stillJan, None)
          _ = assertEquals(nowFeb, Some(id))
        } yield ()
      }
    }
  }

  test("ON CONFLICT on the composite PK (which includes the partition key) upserts through the parent") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          id <- events
            .insert((day = LocalDate.of(2026, 3, 1), kind = "b", payload = "old"))
            .returning(e => e.id)
            .compile
            .unique(s)
          _ <- events
            .insert((id = id, day = LocalDate.of(2026, 3, 1), kind = "b", payload = "new"))
            .onConflictComposite(e => (e.id, e.day))
            .doUpdateFromExcluded((t, ex) => t.payload := ex.payload)
            .compile
            .run(s)
          p <- events.renamed("partitioned_events_default")
            .select(e => e.payload)
            .where(e => e.id === Param.bind(id))
            .compile
            .unique(s)
          _ = assertEquals(p, "new")
        } yield ()
      }
    }
  }
}
