package skunk.sharp.example.api

import cats.data.NonEmptyList
import cats.syntax.all.*
import io.github.arainko.ducktape.*
import skunk.postgis.{Coordinate, Point, SRID}
import skunk.sharp.contrib.citext.Citext
import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.data.Range
import skunk.sharp.example.domain.{BookingRow, BuildingRow, RoomRow}
import skunk.sharp.example.repository.{BookingFilter, BuildingFilter, RoomFilter, SearchRepository}
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate
import java.util.UUID

object Transformers {

  // ---------- Buildings -----------------------------------------------------------------------

  extension (row: BuildingRow)

    def toResponse: BuildingResponse =
      BuildingResponse(
        id = row.id,
        name = row.name,
        address = row.address,
        location = pointToLatLon(row.geom)
      )

  extension (req: CreateBuildingRequest)

    def toRow: BuildingRow.Create =
      BuildingRow.Create(
        name = req.name,
        address = req.address,
        geom = latLonToPoint(req.location)
      )

  extension (req: PatchBuildingRequest)

    def toRow: BuildingRow.Patch =
      BuildingRow.Patch(
        name = req.name,
        address = req.address,
        geom = req.location.map(latLonToPoint)
      )

  extension (q: BuildingFilterQuery)

    def toFilters: List[BuildingFilter] = {
      val within = (q.nearLat, q.nearLon, q.radiusMeters).tupled.map { case (lat, lon, m) =>
        BuildingFilter.WithinMetersOf(lat, lon, m)
      }
      List(
        q.nameContains.map(BuildingFilter.NameContains(_)),
        NonEmptyList.fromList(q.ids).map(BuildingFilter.IdsIn(_)),
        within
      ).flatten
    }

  // ---------- Rooms ---------------------------------------------------------------------------

  extension (row: RoomRow)

    def toResponse: RoomResponse =
      row.into[RoomResponse]
        .transform(
          Field.renamed(_.buildingId, _.building_id),
          // LTree <: String, so already a String on the wire side; spell the cast out for clarity.
          Field.computed(_.location, r => r.location: String),
          Field.computed(_.amenities, r => amenitiesToWire(r.amenities))
        )

  extension (req: SyncRoomRequest)
    def toRow: RoomRow.Sync = RoomRow.Sync(req.name, req.capacity)

  extension (req: CreateRoomRequest)

    /**
     * The buildingId comes from the URL path, not the request body — pass it explicitly. `location` is client input, so
     * an invalid ltree path is a `Left` (→ 400), not an exception.
     */
    def toRow(buildingId: UUID): Either[String, RoomRow.Create] =
      LTree.from(req.location).map(location =>
        RoomRow.Create(
          building_id = buildingId,
          name = req.name,
          capacity = req.capacity,
          location = location,
          amenities = amenitiesFromWire(req.amenities)
        )
      )

  extension (req: PatchRoomRequest)
    def toRow: RoomRow.Patch = req.to[RoomRow.Patch]

  extension (q: RoomFilterQuery)

    /** `Left` if `locationUnder` isn't a valid ltree path (client input → 400). */
    def toFilters: Either[String, List[RoomFilter]] =
      q.locationUnder.traverse(LTree.from).map(location =>
        List(
          q.minCapacity.map(RoomFilter.CapacityAtLeast(_)),
          q.maxCapacity.map(RoomFilter.CapacityAtMost(_)),
          q.nameContains.map(RoomFilter.NameContains(_)),
          NonEmptyList.fromList(q.names).map(RoomFilter.NamesIn(_)),
          NonEmptyList.fromList(q.ids).map(RoomFilter.IdsIn(_)),
          location.map(RoomFilter.LocationUnder(_)),
          q.hasAmenity.map(RoomFilter.HasAmenity(_))
        ).flatten
      )

  // ---------- Bookings ------------------------------------------------------------------------

  extension (row: BookingRow)

    def toResponse: BookingResponse =
      row.into[BookingResponse]
        .transform(
          Field.renamed(_.roomId, _.room_id),
          Field.computed(_.bookerName, b => b.booker_name: String),
          Field.renamed(_.createdAt, _.created_at),
          Field.computed(_.startDate, b => rangeStart(b.period)),
          Field.computed(_.endDate, b => rangeEnd(b.period))
        )

  extension (req: CreateBookingRequest)

    /** `Left` when `startDate` is after `endDate` (client input → 400, instead of a Postgres range error). */
    def toRow: Either[String, BookingRow.Create] =
      PgRange.from(lower = Some(req.startDate), upper = Some(req.endDate)).map(period =>
        req.into[BookingRow.Create]
          .transform(
            Field.renamed(_.room_id, _.roomId),
            Field.computed(_.booker_name, r => Citext(r.bookerName)),
            Field.const(_.period, period)
          )
      )

  extension (q: BookingFilterQuery)

    /** `Left` when `overlapsFrom` is after `overlapsTo` (client input → 400). */
    def toFilters: Either[String, List[BookingFilter]] =
      (q.overlapsFrom, q.overlapsTo).tupled
        .traverse((f, t) => PgRange.from(lower = Some(f), upper = Some(t)))
        .map(overlap =>
          List(
            NonEmptyList.fromList(q.roomIds).map(BookingFilter.RoomsIn(_)),
            q.bookerNameContains.map(BookingFilter.BookerNameContains(_)),
            q.bookerNameSimilar.map(BookingFilter.BookerNameSimilar(_)),
            q.titleContains.map(BookingFilter.TitleContains(_)),
            overlap.map(BookingFilter.OverlapsPeriod(_)),
            q.startsOnOrAfter.map(BookingFilter.StartsOnOrAfter(_)),
            q.endsOnOrBefore.map(BookingFilter.EndsOnOrBefore(_))
          ).flatten
        )

  // ---------- Search --------------------------------------------------------------------------

  extension (row: SearchRepository.RoomWithBuilding)

    def toResponse: RoomWithBuildingResponse = RoomWithBuildingResponse(
      id = row.roomId,
      buildingId = row.buildingId,
      buildingName = row.buildingName,
      buildingLocation = pointToLatLon(row.buildingGeom),
      name = row.name,
      capacity = row.capacity,
      location = row.location: String,
      amenities = amenitiesToWire(row.amenities)
    )

  extension (row: SearchRepository.BuildingAvailability)

    def toResponse: BuildingAvailabilityResponse = BuildingAvailabilityResponse(
      id = row.id,
      name = row.name,
      address = row.address,
      location = pointToLatLon(row.geom),
      freeRoomCount = row.freeRoomCount
    )

  extension (q: RoomSearchQuery)

    /** Project the room-search bundle onto the [[RoomFilter]] ADT — same shape as `RoomFilterQuery.toFilters`. */
    def toRoomFilters: Either[String, List[RoomFilter]] =
      q.locationUnder.traverse(LTree.from).map(location =>
        List(
          q.minCapacity.map(RoomFilter.CapacityAtLeast(_)),
          q.maxCapacity.map(RoomFilter.CapacityAtMost(_)),
          q.nameContains.map(RoomFilter.NameContains(_)),
          location.map(RoomFilter.LocationUnder(_)),
          q.hasAmenity.map(RoomFilter.HasAmenity(_))
        ).flatten
      )

    /** Available-during period — both bounds required (as for booking filters); `Left` if `from` is after `to`. */
    def availableDuring: Either[String, Option[PgRange[LocalDate]]] =
      (q.availableFrom, q.availableTo).tupled.traverse((f, t) => PgRange.from(lower = Some(f), upper = Some(t)))

  extension (q: AvailabilityQuery)

    /** The requested period; `Left` if `from` is after `to`. */
    def period: Either[String, PgRange[LocalDate]] = PgRange.from(lower = Some(q.from), upper = Some(q.to))

  // ---------- Helpers -------------------------------------------------------------------------

  /** PostGIS Points use `(x, y) = (lon, lat)`; convert from the user-facing lat/lon DTO. SRID is always 4326. */
  private val srid4326: SRID = SRID(4326)

  private def latLonToPoint(l: LatLon): Point =
    Point(Some(srid4326), Coordinate.xy(l.lon, l.lat))

  private def pointToLatLon(p: Point): LatLon =
    LatLon(lat = p.coordinate.y, lon = p.coordinate.x)

  private def rangeStart(r: PgRange[LocalDate]): LocalDate = r match {
    case Range.Bounds(Some(lo), _, _, _) => lo
    case Range.Bounds(None, _, _, _)     => LocalDate.MIN
    case Range.Empty                     => LocalDate.MIN
  }

  private def rangeEnd(r: PgRange[LocalDate]): LocalDate = r match {
    case Range.Bounds(_, Some(hi), _, _) => hi
    case Range.Bounds(_, None, _, _)     => LocalDate.MAX
    case Range.Empty                     => LocalDate.MAX
  }

  private def amenitiesToWire(h: Hstore): Map[String, String] =
    h.collect { case (k, Some(v)) => k -> v }.toMap

  private def amenitiesFromWire(m: Map[String, String]): Hstore =
    Hstore(m.view.mapValues(v => Some(v): Option[String]).toMap)

}
