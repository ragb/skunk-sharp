package skunk.sharp.example.api

import io.circe.Codec
import sttp.tapir.Schema
import sttp.tapir.EndpointIO.annotations.query

import java.time.{LocalDate, OffsetDateTime}
import java.util.UUID

/** Lat/lon point on the wire. Maps onto a PostGIS `Point` (SRID 4326) inside the domain. */
case class LatLon(lat: Double, lon: Double) derives Codec.AsObject, Schema

// ---------- Buildings ---------------------------------------------------------------------------

case class BuildingResponse(id: UUID, name: String, address: String, location: LatLon)
    derives Codec.AsObject, Schema

case class CreateBuildingRequest(name: String, address: String, location: LatLon)
    derives Codec.AsObject, Schema

case class PatchBuildingRequest(name: Option[String], address: Option[String], location: Option[LatLon])
    derives Codec.AsObject, Schema

/**
 * Query-parameter bundle for `GET /api/v1/buildings`. The trio `nearLat` / `nearLon` / `radiusMeters` ties together to
 * form a `WithinMetersOf` filter; the spatial primitive (`ST_DWithin`) wants all three or none. The routing layer
 * emits the filter only when all three are present.
 */
case class BuildingFilterQuery(
  @query nameContains: Option[String],
  @query ids: List[UUID],
  @query nearLat: Option[Double],
  @query nearLon: Option[Double],
  @query radiusMeters: Option[Double]
)

object BuildingFilterQuery {
  val empty: BuildingFilterQuery = BuildingFilterQuery(None, Nil, None, None, None)
}

// ---------- Rooms (nested under buildings) ------------------------------------------------------

case class RoomResponse(
  id: UUID,
  buildingId: UUID,
  name: String,
  capacity: Int,
  location: String,
  amenities: Map[String, String]
) derives Codec.AsObject, Schema

/** Body for `POST /api/v1/buildings/{buildingId}/rooms` — the buildingId comes from the URL, not from the body. */
case class CreateRoomRequest(
  name: String,
  capacity: Int,
  location: String,
  amenities: Map[String, String]
) derives Codec.AsObject, Schema

case class PatchRoomRequest(name: Option[String], capacity: Option[Int]) derives Codec.AsObject, Schema

/**
 * Query-parameter bundle for `GET /api/v1/buildings/{buildingId}/rooms`. The buildingId is on the URL path, so the
 * filter bundle only carries within-building criteria.
 */
case class RoomFilterQuery(
  @query minCapacity: Option[Int],
  @query maxCapacity: Option[Int],
  @query nameContains: Option[String],
  @query names: List[String],
  @query ids: List[UUID],
  @query locationUnder: Option[String],
  @query hasAmenity: Option[String]
)

object RoomFilterQuery {

  /** No filters supplied — handy for tests or default routing. */
  val empty: RoomFilterQuery = RoomFilterQuery(None, None, None, Nil, Nil, None, None)
}

// ---------- Bookings (cross-building; keep flat) ------------------------------------------------

case class BookingResponse(
  id: UUID,
  roomId: UUID,
  bookerName: String,
  title: String,
  startDate: LocalDate,
  endDate: LocalDate,
  createdAt: OffsetDateTime
) derives Codec.AsObject, Schema

case class CreateBookingRequest(
  roomId: UUID,
  bookerName: String,
  title: String,
  startDate: LocalDate,
  endDate: LocalDate
) derives Codec.AsObject, Schema

case class ApiError(message: String) derives Codec.AsObject, Schema

case class BookingFilterQuery(
  @query roomIds: List[UUID],
  @query bookerNameContains: Option[String],
  @query bookerNameSimilar: Option[String],
  @query titleContains: Option[String],
  @query overlapsFrom: Option[LocalDate],
  @query overlapsTo: Option[LocalDate],
  @query startsOnOrAfter: Option[LocalDate],
  @query endsOnOrBefore: Option[LocalDate]
)

object BookingFilterQuery {
  val empty: BookingFilterQuery = BookingFilterQuery(Nil, None, None, None, None, None, None, None)
}
