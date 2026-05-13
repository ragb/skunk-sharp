package skunk.sharp.example.api

import io.circe.Codec
import sttp.tapir.Schema
import sttp.tapir.EndpointIO.annotations.query

import java.time.{LocalDate, OffsetDateTime}
import java.util.UUID

case class RoomResponse(
  id: UUID,
  name: String,
  capacity: Int,
  location: String,
  amenities: Map[String, String]
) derives Codec.AsObject, Schema

case class CreateRoomRequest(
  name: String,
  capacity: Int,
  location: String,
  amenities: Map[String, String]
) derives Codec.AsObject, Schema

case class PatchRoomRequest(name: Option[String], capacity: Option[Int]) derives Codec.AsObject, Schema

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

/**
 * Query-parameter bundle for `GET /api/v1/rooms`. Each `@query`-annotated field maps to one query string key (the field
 * name doubles as the param name); tapir's `EndpointInput.derived` flattens the case class into a single composed
 * `EndpointInput`. Field types drive parsing (`Option[T]` for one-or-zero, `List[T]` for repeated keys like
 * `?names=a&names=b`). Routes translate this DTO to a `List[RoomFilter]` via `.toFilters`.
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

/**
 * Query-parameter bundle for `GET /api/v1/bookings`. Mirrors [[RoomFilterQuery]] in shape and intent.
 *
 * `overlapsFrom` and `overlapsTo` are paired: only when both are present does the routing layer turn them into a single
 * `OverlapsPeriod` filter. The half-bounded `startsOnOrAfter` / `endsOnOrBefore` are independent.
 */
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
