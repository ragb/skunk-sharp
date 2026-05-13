package skunk.sharp.example.api

import cats.data.NonEmptyList
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.example.repository.{BookingFilter, BuildingFilter, RoomFilter}
import Transformers.*

import java.time.LocalDate
import java.util.UUID

/**
 * Pure unit tests for the query DTO ↔ filter ADT projection. The SQL-rendering side of each filter case is already
 * covered by core suites; what's specific to the example module is the `q.toFilters` extension's logic, especially
 * the paired-field rules (`overlapsFrom`/`overlapsTo`, `nearLat`/`nearLon`/`radiusMeters`).
 */
class TransformersFilterSuite extends munit.FunSuite {

  // ---------- BuildingFilterQuery ---------------------------------------------------------------

  test("BuildingFilterQuery.empty.toFilters is empty") {
    assertEquals(BuildingFilterQuery.empty.toFilters, Nil)
  }

  test("BuildingFilterQuery — every field translates to its filter case") {
    val ids = List(UUID.randomUUID, UUID.randomUUID)
    val q   = BuildingFilterQuery(
      nameContains = Some("hq"),
      ids = ids,
      nearLat = Some(53.34),
      nearLon = Some(-6.26),
      radiusMeters = Some(5000.0)
    )
    assertEquals(
      q.toFilters,
      List(
        BuildingFilter.NameContains("hq"),
        BuildingFilter.IdsIn(NonEmptyList.fromListUnsafe(ids)),
        BuildingFilter.WithinMetersOf(53.34, -6.26, 5000.0)
      )
    )
  }

  test("BuildingFilterQuery — partial nearLat without nearLon drops WithinMetersOf") {
    val q = BuildingFilterQuery.empty.copy(nearLat = Some(53.34))
    assertEquals(q.toFilters, Nil)
  }

  // ---------- RoomFilterQuery -------------------------------------------------------------------

  test("RoomFilterQuery.empty.toFilters is empty") {
    assertEquals(RoomFilterQuery.empty.toFilters, Nil)
  }

  test("RoomFilterQuery — every field translates to its filter case") {
    val ids = List(UUID.randomUUID, UUID.randomUUID)
    val q   = RoomFilterQuery(
      minCapacity = Some(10),
      maxCapacity = Some(50),
      nameContains = Some("hub"),
      names = List("alpha", "beta"),
      ids = ids,
      locationUnder = Some("acme.dublin"),
      hasAmenity = Some("projector")
    )
    assertEquals(
      q.toFilters,
      List(
        RoomFilter.CapacityAtLeast(10),
        RoomFilter.CapacityAtMost(50),
        RoomFilter.NameContains("hub"),
        RoomFilter.NamesIn(NonEmptyList.of("alpha", "beta")),
        RoomFilter.IdsIn(NonEmptyList.fromListUnsafe(ids)),
        RoomFilter.LocationUnder(LTree("acme.dublin")),
        RoomFilter.HasAmenity("projector")
      )
    )
  }

  test("RoomFilterQuery — empty multi-value lists drop out (no IN clause)") {
    val q = RoomFilterQuery.empty.copy(minCapacity = Some(1))
    assertEquals(q.toFilters, List(RoomFilter.CapacityAtLeast(1)))
  }

  // ---------- BookingFilterQuery ----------------------------------------------------------------

  test("BookingFilterQuery.empty.toFilters is empty") {
    assertEquals(BookingFilterQuery.empty.toFilters, Nil)
  }

  test("BookingFilterQuery — every field translates to its filter case") {
    val rids = List(UUID.randomUUID, UUID.randomUUID)
    val from = LocalDate.parse("2024-01-01")
    val to   = LocalDate.parse("2024-12-31")
    val q    = BookingFilterQuery(
      roomIds = rids,
      bookerNameContains = Some("alice"),
      bookerNameSimilar = Some("alyce"),
      titleContains = Some("standup"),
      overlapsFrom = Some(from),
      overlapsTo = Some(to),
      startsOnOrAfter = Some(from),
      endsOnOrBefore = Some(to)
    )
    assertEquals(
      q.toFilters,
      List(
        BookingFilter.RoomsIn(NonEmptyList.fromListUnsafe(rids)),
        BookingFilter.BookerNameContains("alice"),
        BookingFilter.BookerNameSimilar("alyce"),
        BookingFilter.TitleContains("standup"),
        BookingFilter.OverlapsPeriod(from, to),
        BookingFilter.StartsOnOrAfter(from),
        BookingFilter.EndsOnOrBefore(to)
      )
    )
  }

  test("BookingFilterQuery — overlapsFrom alone drops the OverlapsPeriod filter") {
    val q = BookingFilterQuery.empty.copy(overlapsFrom = Some(LocalDate.parse("2024-01-01")))
    assertEquals(q.toFilters, Nil)
  }

  test("BookingFilterQuery — overlapsTo alone drops OverlapsPeriod too") {
    val q = BookingFilterQuery.empty.copy(overlapsTo = Some(LocalDate.parse("2024-12-31")))
    assertEquals(q.toFilters, Nil)
  }

  test("BookingFilterQuery — half-bounded startsOnOrAfter is independent of the OverlapsPeriod pair") {
    val date = LocalDate.parse("2024-06-15")
    val q    = BookingFilterQuery.empty.copy(startsOnOrAfter = Some(date))
    assertEquals(q.toFilters, List(BookingFilter.StartsOnOrAfter(date)))
  }
}
