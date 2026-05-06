package skunk.sharp.example.api

import sttp.tapir.*
import sttp.tapir.json.circe.*
import sttp.model.StatusCode

import java.util.UUID

object Endpoints {

  // Error type is (StatusCode, ApiError) for all endpoints.
  // Server logic returns Left((StatusCode.NotFound, ApiError("..."))) for 404 etc.
  private val base = endpoint.errorOut(statusCode and jsonBody[ApiError])

  // Filter query bundles: each `@query`-annotated field becomes one query string key. Tapir derives a single
  // composed `EndpointInput[FilterQuery]` so endpoints receive one typed value rather than a 5- or 7-tuple.
  private val roomFilterInput:    EndpointInput[RoomFilterQuery]    = EndpointInput.derived[RoomFilterQuery]
  private val bookingFilterInput: EndpointInput[BookingFilterQuery] = EndpointInput.derived[BookingFilterQuery]

  object rooms {

    /**
     * GET /api/v1/rooms — list with optional filters bundled in [[RoomFilterQuery]]. Absent fields drop out of
     * the WHERE; present fields AND-combine. Repeated keys (`?names=a&names=b`) produce a non-empty list which
     * the routing layer turns into an `IN (…)` clause.
     */
    val list =
      base.get
        .in("api" / "v1" / "rooms")
        .in(roomFilterInput)
        .out(jsonBody[List[RoomResponse]])

    val getById =
      base.get
        .in("api" / "v1" / "rooms" / path[UUID]("id"))
        .out(jsonBody[RoomResponse])

    val create =
      base.post
        .in("api" / "v1" / "rooms")
        .in(jsonBody[CreateRoomRequest])
        .out(statusCode(StatusCode.Created) and jsonBody[RoomResponse])

    val patch =
      base.patch
        .in("api" / "v1" / "rooms" / path[UUID]("id"))
        .in(jsonBody[PatchRoomRequest])
        .out(jsonBody[RoomResponse])

    val delete =
      base.delete
        .in("api" / "v1" / "rooms" / path[UUID]("id"))
        .out(statusCode(StatusCode.NoContent))

    val all = List(list, getById, create, patch, delete)
  }

  object bookings {

    /**
     * GET /api/v1/bookings — list with optional filters bundled in [[BookingFilterQuery]]. `overlapsFrom` and
     * `overlapsTo` together form an `OverlapsPeriod` filter (only applied when both are present); the
     * one-sided `startsOnOrAfter` / `endsOnOrBefore` are independent.
     */
    val list =
      base.get
        .in("api" / "v1" / "bookings")
        .in(bookingFilterInput)
        .out(jsonBody[List[BookingResponse]])

    val getById =
      base.get
        .in("api" / "v1" / "bookings" / path[UUID]("id"))
        .out(jsonBody[BookingResponse])

    val byRoom =
      base.get
        .in("api" / "v1" / "rooms" / path[UUID]("roomId") / "bookings")
        .out(jsonBody[List[BookingResponse]])

    val create =
      base.post
        .in("api" / "v1" / "bookings")
        .in(jsonBody[CreateBookingRequest])
        .out(statusCode(StatusCode.Created) and jsonBody[BookingResponse])

    val delete =
      base.delete
        .in("api" / "v1" / "bookings" / path[UUID]("id"))
        .out(statusCode(StatusCode.NoContent))

    val all = List(list, getById, byRoom, create, delete)
  }

  val all = rooms.all ++ bookings.all
}
