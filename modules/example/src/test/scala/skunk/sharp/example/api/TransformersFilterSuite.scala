package skunk.sharp.example.api

import cats.data.NonEmptyList
import skunk.sharp.example.repository.{BookingFilter, RoomFilter}
import Transformers.*

import java.time.LocalDate
import java.util.UUID

/**
 * Pure unit tests for the query DTO ↔ filter ADT projection. The SQL-rendering side of each filter case is
 * already covered by core suites (`WhereSuite`, `ParamSuite`, `RangeSuite`) — what's specific to the example
 * module is the `q.toFilters` extension's logic, especially the paired-field rule for `OverlapsPeriod`.
 */
class TransformersFilterSuite extends munit.FunSuite {

  test("RoomFilterQuery.empty.toFilters is empty") {
    assertEquals(RoomFilterQuery.empty.toFilters, Nil)
  }

  test("RoomFilterQuery — every field translates to its filter case") {
    val ids = List(UUID.randomUUID, UUID.randomUUID)
    val q = RoomFilterQuery(
      minCapacity  = Some(10),
      maxCapacity  = Some(50),
      nameContains = Some("hub"),
      names        = List("alpha", "beta"),
      ids          = ids
    )
    assertEquals(
      q.toFilters,
      List(
        RoomFilter.CapacityAtLeast(10),
        RoomFilter.CapacityAtMost(50),
        RoomFilter.NameContains("hub"),
        RoomFilter.NamesIn(NonEmptyList.of("alpha", "beta")),
        RoomFilter.IdsIn(NonEmptyList.fromListUnsafe(ids))
      )
    )
  }

  test("RoomFilterQuery — empty multi-value lists drop out (no IN clause)") {
    val q = RoomFilterQuery(
      minCapacity  = Some(1),
      maxCapacity  = None,
      nameContains = None,
      names        = Nil,
      ids          = Nil
    )
    assertEquals(q.toFilters, List(RoomFilter.CapacityAtLeast(1)))
  }

  test("BookingFilterQuery.empty.toFilters is empty") {
    assertEquals(BookingFilterQuery.empty.toFilters, Nil)
  }

  test("BookingFilterQuery — every field translates to its filter case") {
    val rids = List(UUID.randomUUID, UUID.randomUUID)
    val from = LocalDate.parse("2024-01-01")
    val to   = LocalDate.parse("2024-12-31")
    val q = BookingFilterQuery(
      roomIds            = rids,
      bookerNameContains = Some("alice"),
      titleContains      = Some("standup"),
      overlapsFrom       = Some(from),
      overlapsTo         = Some(to),
      startsOnOrAfter    = Some(from),
      endsOnOrBefore     = Some(to)
    )
    assertEquals(
      q.toFilters,
      List(
        BookingFilter.RoomsIn(NonEmptyList.fromListUnsafe(rids)),
        BookingFilter.BookerNameContains("alice"),
        BookingFilter.TitleContains("standup"),
        BookingFilter.OverlapsPeriod(from, to),
        BookingFilter.StartsOnOrAfter(from),
        BookingFilter.EndsOnOrBefore(to)
      )
    )
  }

  test("BookingFilterQuery — overlapsFrom alone (without overlapsTo) drops the OverlapsPeriod filter") {
    val q = BookingFilterQuery.empty.copy(overlapsFrom = Some(LocalDate.parse("2024-01-01")))
    assertEquals(q.toFilters, Nil, "single-bound overlaps must not produce an OverlapsPeriod")
  }

  test("BookingFilterQuery — overlapsTo alone drops OverlapsPeriod too") {
    val q = BookingFilterQuery.empty.copy(overlapsTo = Some(LocalDate.parse("2024-12-31")))
    assertEquals(q.toFilters, Nil)
  }

  test("BookingFilterQuery — half-bounded startsOnOrAfter is independent of the OverlapsPeriod pair") {
    val date = LocalDate.parse("2024-06-15")
    val q = BookingFilterQuery.empty.copy(startsOnOrAfter = Some(date))
    assertEquals(q.toFilters, List(BookingFilter.StartsOnOrAfter(date)))
  }
}
