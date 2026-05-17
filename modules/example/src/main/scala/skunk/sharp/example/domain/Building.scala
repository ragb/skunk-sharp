package skunk.sharp.example.domain

import skunk.postgis.Point
import skunk.sharp.dsl.*
import skunk.sharp.postgis.given

import java.util.UUID

/**
 * A physical building hosting one or more rooms. The `geom` is a WGS84 (SRID 4326) `Point`; queries like "within N
 * metres of (lat, lon)" use `ST_DWithin` against this column, with a GiST index from V3 keeping it fast.
 *
 * Rooms reference Buildings via `rooms.building_id` (cascade on delete) so removing a building cleans up its rooms —
 * and via the existing FK on bookings, the bookings beneath them.
 */
case class BuildingRow(id: UUID, name: String, address: String, geom: Point)

object BuildingRow {

  val table = Table.of[BuildingRow]("buildings").withPrimary("id").withDefault("id")

  case class Create(name: String, address: String, geom: Point)
  case class Patch(name: Option[String], address: Option[String], geom: Option[Point])
}
