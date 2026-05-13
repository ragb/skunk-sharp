package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import fs2.Stream
import skunk.Session
import skunk.postgis.Point
import skunk.sharp.*
import skunk.sharp.contrib.hstore.{Hstore, *}
import skunk.sharp.contrib.ltree.{LTree, *}
import skunk.sharp.dsl.*
import skunk.sharp.example.domain.{BookingRow, BuildingRow, RoomRow}
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.postgis.*
import skunk.sharp.postgis.given

import java.time.LocalDate
import java.util.UUID

/**
 * Cross-resource read-side queries: cross-building room search + per-building availability counts. Each method
 * composes a single skunk-sharp query that joins two or three tables, applies spatial / temporal / domain filters,
 * and ships the matching rows back as a stream.
 */
trait SearchRepository {

  /**
   * Rooms within a radius of `(lat, lon)` that also match the supplied room filters, returned with their parent
   * building info. The query is `rooms INNER JOIN buildings ON rooms.building_id = buildings.id` filtered by
   * `ST_DWithin(buildings.geom, …)` and ordered by `ST_Distance(buildings.geom, probe) ASC`.
   *
   * Available-during is optional — when both bounds are given, rooms with an overlapping booking are dropped via
   * `NOT EXISTS`.
   */
  def findRoomsNear(
    near: (Double, Double),
    radius: Double,
    roomFilters: List[RoomFilter],
    availableDuring: Option[(LocalDate, LocalDate)]
  ): Kleisli[Stream[IO, *], Session[IO], SearchRepository.RoomWithBuilding]

  /**
   * Buildings within a radius of `(lat, lon)`, with the count of rooms that are NOT booked during `[from, to)`.
   * Ordered by free-room count descending — perfect for a "find a meeting room" landing page.
   */
  def buildingsWithAvailability(
    near: (Double, Double),
    radius: Double,
    from: LocalDate,
    to: LocalDate
  ): Kleisli[Stream[IO, *], Session[IO], SearchRepository.BuildingAvailability]

}

object SearchRepository {

  /** Wide row returned by [[findRoomsNear]] — every column from rooms plus its building's id/name/geom. */
  final case class RoomWithBuilding(
    roomId: UUID,
    buildingId: UUID,
    buildingName: String,
    buildingGeom: Point,
    name: String,
    capacity: Int,
    location: LTree,
    amenities: Hstore
  )

  /** Aggregated row returned by [[buildingsWithAvailability]]. */
  final case class BuildingAvailability(
    id: UUID,
    name: String,
    address: String,
    geom: Point,
    freeRoomCount: Long
  )

  val live: SearchRepository = new SearchRepository {

    private val rooms     = RoomRow.table
    private val buildings = BuildingRow.table
    private val bookings  = BookingRow.table

    private val roomsCv = rooms.columnsView

    /** Translate a room filter to a Where on the unqualified rooms view. Same logic as [[RoomRepository]]'s arm. */
    private def roomWhere(f: RoomFilter): Where[skunk.Void] = f match {
      case RoomFilter.CapacityAtLeast(n)    => roomsCv.capacity >= Param.bind(n)
      case RoomFilter.CapacityAtMost(n)     => roomsCv.capacity <= Param.bind(n)
      case RoomFilter.NameContains(s)       => roomsCv.name.ilike(Param.bind(s"%$s%"))
      case RoomFilter.NamesIn(ns)           => roomsCv.name.in(ns.map(Param.bind(_)))
      case RoomFilter.IdsIn(ids)            => roomsCv.id.in(ids.map(Param.bind(_)))
      case RoomFilter.LocationUnder(prefix) => roomsCv.location.isDescendantOf(Param.bind(prefix))
      case RoomFilter.HasAmenity(key)       => roomsCv.amenities.hasKey(Param.bind(key))
    }

    /** Probe expression `ST_SetSRID(ST_MakePoint($lon, $lat), 4326)` with the values baked via Param.bind. */
    private def probeAt(lat: Double, lon: Double): TypedExpr[skunk.postgis.Geometry, skunk.Void] =
      PgPostgis.setSRID(
        PgPostgis.makePoint(Param.bind(lon), Param.bind(lat)),
        Param.bind(4326)
      )

    def findRoomsNear(
      near: (Double, Double),
      radius: Double,
      roomFilters: List[RoomFilter],
      availableDuring: Option[(LocalDate, LocalDate)]
    ): Kleisli[Stream[IO, *], Session[IO], RoomWithBuilding] = {
      val (lat, lon) = near

      // INNER JOIN rooms × buildings on the FK. Spatial filter goes on buildings.geom; the room filters compose
      // unqualified (they reference `roomsCv` directly — works because the rooms view is unaliased in the join).
      val q = rooms.innerJoin(buildings)
        .on(j => j.rooms.building_id ==== j.buildings.id)
        .select { j =>
          (
            j.rooms.id,
            j.buildings.id,
            j.buildings.name,
            j.buildings.geom,
            j.rooms.name,
            j.rooms.capacity,
            j.rooms.location,
            j.rooms.amenities
          )
        }
        .where { j =>
          val probe       = probeAt(lat, lon)
          val spatial     = j.buildings.geom.dWithin(probe, Param.bind(radius))
          val roomScoped  = roomFilters.map(roomWhere)
          val availFilter = availableDuring.map { case (from, to) =>
            val period = PgRange[LocalDate](lower = Some(from), upper = Some(to))
            Pg.notExists(
              bookings.select(_ => lit(1)).where(bk =>
                bk.room_id ==== j.rooms.id && bk.period.overlaps(Param.bind(period))
              )
            )
          }
          allOf((spatial +: roomScoped ++: availFilter.toList)*)
        }
        .orderBy(j => j.buildings.geom.distance(probeAt(lat, lon)).asc)
        .to[RoomWithBuilding]

      q.compile.streamKF[IO]()
    }

    // LEFT JOIN: a building with zero matching rooms still appears (with free_count = 0). The NOT EXISTS clause
    // on the JOIN's ON predicate filters out rooms whose period overlaps an existing booking, so COUNT(rooms.id)
    // returns just the free ones (NULL → not counted).
    //
    // Fully static — compiled once at object init. Args at execute time = (PgRange[LocalDate], Double, Double, Double)
    // = (period, lon, lat, radius), in the order the `Param`s appear during template construction.
    private val buildingsWithAvailabilityQ = {
      val period = Param[PgRange[LocalDate]]
      val lon    = Param[Double]
      val lat    = Param[Double]
      val radius = Param[Double]
      val probe  = PgPostgis.setSRID(PgPostgis.makePoint(lon, lat), lit(4326))

      buildings.leftJoin(rooms)
        .on { j =>
          j.rooms.building_id ==== j.buildings.id &&
            Pg.notExists(
              bookings.select(_ => lit(1)).where(bk =>
                bk.room_id ==== j.rooms.id && bk.period.overlaps(period)
              )
            )
        }
        .select(j => (
          j.buildings.id,
          j.buildings.name,
          j.buildings.address,
          j.buildings.geom,
          Pg.count(j.rooms.id)
        ))
        .where(j => j.buildings.geom.dWithin(probe, radius))
        .groupBy(j => (j.buildings.id, j.buildings.name, j.buildings.address, j.buildings.geom))
        .orderBy(j => (Pg.count(j.rooms.id).desc, j.buildings.name.asc))
        .to[BuildingAvailability]
        .compile
    }

    def buildingsWithAvailability(
      near: (Double, Double),
      radius: Double,
      from: LocalDate,
      to: LocalDate
    ): Kleisli[Stream[IO, *], Session[IO], BuildingAvailability] = {
      val (lat, lon) = near
      val period     = PgRange[LocalDate](lower = Some(from), upper = Some(to))
      buildingsWithAvailabilityQ.streamKF[IO]((period, lon, lat, radius), 64)
    }
  }
}
