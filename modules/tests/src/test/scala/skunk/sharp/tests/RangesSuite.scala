package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.data.Range

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}

object RangesSuite {
  case class Booking(id: Int, period: PgRange[LocalDate])
  case class Reservation(id: Int, slot: PgRange[OffsetDateTime])
}

class RangesSuite extends PgFixture {
  import RangesSuite.*

  private val bookings     = Table.of[Booking]("bookings").withPrimary("id")
  private val reservations = Table.of[Reservation]("reservations").withPrimary("id")

  // -------- Round-trip: insert and read back ----------------------------------------

  test("round-trip: insert and select a daterange column") {
    withContainers { containers =>
      session(containers).use { s =>
        val period =
          PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = Some(LocalDate.of(2024, 12, 31)))
        for {
          _    <- bookings.insert((id = 1, period = period)).compile.run(s)
          rows <- bookings.select.where(b => b.id === 1).compile.run(s)
          row = rows.head
          // Postgres canonicalises daterange to [lower, upper) — upper becomes 2025-01-01
          _      = assertEquals(row.id, 1)
          _      = assert(row.period.isInstanceOf[Range.Bounds[?]], s"expected Bounds, got ${row.period}")
          bounds = row.period.asInstanceOf[Range.Bounds[LocalDate]]
          _      = assertEquals(bounds.lower, Some(LocalDate.of(2024, 1, 1)))
          _      = assertEquals(bounds.lowerInclusive, true)
        } yield ()
      }
    }
  }

  test("round-trip: insert and select a tstzrange column") {
    withContainers { containers =>
      session(containers).use { s =>
        val t1   = OffsetDateTime.of(2024, 6, 1, 0, 0, 0, 0, ZoneOffset.UTC)
        val t2   = OffsetDateTime.of(2024, 9, 1, 0, 0, 0, 0, ZoneOffset.UTC)
        val slot = PgRange[OffsetDateTime](lower = Some(t1), upper = Some(t2))
        for {
          _    <- reservations.insert((id = 1, slot = slot)).compile.run(s)
          rows <- reservations.select.where(r => r.id === 1).compile.run(s)
          row    = rows.head
          _      = assertEquals(row.id, 1)
          bounds = row.slot.asInstanceOf[Range.Bounds[OffsetDateTime]]
          _      = assertEquals(bounds.lower, Some(t1))
          _      = assertEquals(bounds.upper, Some(t2))
        } yield ()
      }
    }
  }

  test("round-trip: empty range") {
    withContainers { containers =>
      session(containers).use { s =>
        val empty = PgRange.empty[LocalDate]
        for {
          _    <- bookings.insert((id = 2, period = empty)).compile.run(s)
          rows <- bookings.select.where(b => b.id === 2).compile.run(s)
          row = rows.head
          _   = assert(row.period == Range.Empty, s"expected Empty, got ${row.period}")
        } yield ()
      }
    }
  }

  // -------- Operators --------------------------------------------------------------

  test("&& overlaps — filter bookings overlapping a probe range") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- bookings.insert.values(
            (
              id = 10,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = Some(LocalDate.of(2024, 6, 30)))
            ),
            (
              id = 11,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 7, 1)), upper = Some(LocalDate.of(2024, 12, 31)))
            ),
            (
              id = 12,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 4, 1)), upper = Some(LocalDate.of(2024, 9, 30)))
            )
          ).compile.run(s)
          // probe ends 2024-07-01 exclusive, so id=11 (starts 2024-07-01) does not overlap
          probe = PgRange[LocalDate](lower = Some(LocalDate.of(2024, 5, 1)), upper = Some(LocalDate.of(2024, 7, 1)))
          ids <- bookings
            .select(b => b.id)
            .where(b => b.period.overlaps(param(probe)))
            .where(b => b.id.in(NonEmptyList.of(10, 11, 12)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(10, 12))
        } yield ()
      }
    }
  }

  test("@> containsElem — find bookings covering a specific date") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- bookings.insert.values(
            (
              id = 20,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = Some(LocalDate.of(2024, 6, 30)))
            ),
            (
              id = 21,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 7, 1)), upper = Some(LocalDate.of(2024, 12, 31)))
            )
          ).compile.run(s)
          target = LocalDate.of(2024, 3, 15)
          ids <- bookings
            .select(b => b.id)
            .where(b => b.period.containsElem(param(target)))
            .where(b => b.id.in(NonEmptyList.of(20, 21)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(20))
        } yield ()
      }
    }
  }

  test("@> contains range — period that fully contains another") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- bookings.insert.values(
            (
              id = 30,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = Some(LocalDate.of(2024, 12, 31)))
            ),
            (
              id = 31,
              period =
                PgRange[LocalDate](lower = Some(LocalDate.of(2024, 6, 1)), upper = Some(LocalDate.of(2024, 7, 31)))
            )
          ).compile.run(s)
          // id=31 is a strict sub-range of id=30; only id=30 contains sub (id=31 IS sub — self-contains is true, both would pass @>)
          // Use a sub that is strictly inside id=30 but not equal to id=31
          sub = PgRange[LocalDate](lower = Some(LocalDate.of(2024, 3, 1)), upper = Some(LocalDate.of(2024, 10, 1)))
          ids <- bookings
            .select(b => b.id)
            .where(b => b.period.contains(param(sub)))
            .where(b => b.id.in(NonEmptyList.of(30, 31)))
            .compile.run(s)
          _ = assertEquals(ids.toSet, Set(30))
        } yield ()
      }
    }
  }

  // -------- Accessor functions -----------------------------------------------------

  test("rangeLower / rangeUpper return endpoint values") {
    withContainers { containers =>
      session(containers).use { s =>
        val period = PgRange[LocalDate](lower = Some(LocalDate.of(2024, 3, 1)), upper = Some(LocalDate.of(2024, 9, 1)))
        for {
          _    <- bookings.insert((id = 40, period = period)).compile.run(s)
          rows <- bookings
            .select(b => (Pg.rangeLower(b.period), Pg.rangeUpper(b.period)))
            .where(b => b.id === 40)
            .compile.run(s)
          (lo, hi) = rows.head
          _        = assertEquals(lo, Some(LocalDate.of(2024, 3, 1)))
          _        = assertEquals(hi, Some(LocalDate.of(2024, 9, 1)))
        } yield ()
      }
    }
  }

  test("rangeIsEmpty / rangeLowerInf / rangeUpperInf") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- bookings.insert.values(
            (id = 50, period = PgRange.empty[LocalDate]),
            (id = 51, period = PgRange[LocalDate](lower = None, upper = Some(LocalDate.of(2024, 1, 1)))),
            (id = 52, period = PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = None))
          ).compile.run(s)
          rows <- bookings
            .select(b => (b.id, Pg.rangeIsEmpty(b.period), Pg.rangeLowerInf(b.period), Pg.rangeUpperInf(b.period)))
            .where(b => b.id.in(NonEmptyList.of(50, 51, 52)))
            .compile.run(s)
            .map(_.sortBy(_._1))
          _ = assertEquals(rows(0)._2, true)  // id=50 is empty
          _ = assertEquals(rows(1)._4, false) // id=51 upper_inf false
          _ = assertEquals(rows(1)._3, true)  // id=51 lower_inf true
          _ = assertEquals(rows(2)._4, true)  // id=52 upper_inf true
          _ = assertEquals(rows(2)._3, false) // id=52 lower_inf false
        } yield ()
      }
    }
  }

  // -------- Constructor function ---------------------------------------------------

  test("Pg.daterange constructor — build and compare a range from params") {
    withContainers { containers =>
      session(containers).use { s =>
        val lo = LocalDate.of(2024, 1, 1)
        val hi = LocalDate.of(2024, 12, 31)
        for {
          _ <-
            bookings.insert((id = 60, period = PgRange[LocalDate](lower = Some(lo), upper = Some(hi)))).compile.run(s)
          ids <- bookings
            .select(b => b.id)
            .where(b => b.period.overlaps(Pg.daterange(param(lo), param(hi))))
            .where(b => b.id === 60)
            .compile.run(s)
          _ = assertEquals(ids.toList, List(60))
        } yield ()
      }
    }
  }

}
