package skunk.sharp.example.repository

import cats.data.NonEmptyList

import java.util.UUID

/**
 * Domain-level filter ADT for building listing. Mirrors [[RoomFilter]] / [[BookingFilter]]; the repository materialises
 * a `List[BuildingFilter]` into a single AND-combined WHERE.
 */
sealed trait BuildingFilter

object BuildingFilter {

  /** `name ILIKE '%substring%'` — case-insensitive substring match on building name. */
  final case class NameContains(substring: String) extends BuildingFilter

  /** `id IN (…)` — restrict to a non-empty set of ids. */
  final case class IdsIn(values: NonEmptyList[UUID]) extends BuildingFilter

  /**
   * `ST_DWithin(geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326), meters)` — buildings within `meters` of the given
   * lat/lon. The 4326 SRID is taken to match the column type; PostGIS does the spherical distance math when the column
   * is geography, or planar math when it's geometry — we use geometry here, fine for short distances at non-extreme
   * latitudes.
   */
  final case class WithinMetersOf(lat: Double, lon: Double, meters: Double) extends BuildingFilter
}
