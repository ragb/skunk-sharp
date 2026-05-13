package skunk.sharp.example.api

import sttp.tapir.*
import sttp.tapir.json.circe.*
import sttp.model.StatusCode

import java.util.UUID

object Endpoints {

  // Error type is (StatusCode, ApiError) for all endpoints.
  private val base = endpoint.errorOut(statusCode and jsonBody[ApiError])

  private val buildingFilterInput: EndpointInput[BuildingFilterQuery] = EndpointInput.derived[BuildingFilterQuery]
  private val roomFilterInput: EndpointInput[RoomFilterQuery]         = EndpointInput.derived[RoomFilterQuery]
  private val bookingFilterInput: EndpointInput[BookingFilterQuery]   = EndpointInput.derived[BookingFilterQuery]
  private val roomSearchInput: EndpointInput[RoomSearchQuery]         = EndpointInput.derived[RoomSearchQuery]
  private val availabilityInput: EndpointInput[AvailabilityQuery]     = EndpointInput.derived[AvailabilityQuery]

  // ---- Buildings ---------------------------------------------------------------------------

  object buildings {

    /**
     * GET /api/v1/buildings — list with optional filters. The trio `nearLat` / `nearLon` / `radiusMeters` activates a
     * `ST_DWithin`-backed proximity filter when all three are present.
     */
    val list =
      base.get
        .in("api" / "v1" / "buildings")
        .in(buildingFilterInput)
        .out(jsonBody[List[BuildingResponse]])

    val getById =
      base.get
        .in("api" / "v1" / "buildings" / path[UUID]("id"))
        .out(jsonBody[BuildingResponse])

    val create =
      base.post
        .in("api" / "v1" / "buildings")
        .in(jsonBody[CreateBuildingRequest])
        .out(statusCode(StatusCode.Created) and jsonBody[BuildingResponse])

    val patch =
      base.patch
        .in("api" / "v1" / "buildings" / path[UUID]("id"))
        .in(jsonBody[PatchBuildingRequest])
        .out(jsonBody[BuildingResponse])

    val delete =
      base.delete
        .in("api" / "v1" / "buildings" / path[UUID]("id"))
        .out(statusCode(StatusCode.NoContent))

    val all = List(list, getById, create, patch, delete)
  }

  // ---- Rooms (nested under a building) ------------------------------------------------------

  object rooms {

    private val basePath = "api" / "v1" / "buildings" / path[UUID]("buildingId") / "rooms"

    /** GET /api/v1/buildings/{buildingId}/rooms — within-building filters from [[RoomFilterQuery]]. */
    val list =
      base.get
        .in(basePath)
        .in(roomFilterInput)
        .out(jsonBody[List[RoomResponse]])

    val getById =
      base.get
        .in(basePath / path[UUID]("id"))
        .out(jsonBody[RoomResponse])

    val create =
      base.post
        .in(basePath)
        .in(jsonBody[CreateRoomRequest])
        .out(statusCode(StatusCode.Created) and jsonBody[RoomResponse])

    val patch =
      base.patch
        .in(basePath / path[UUID]("id"))
        .in(jsonBody[PatchRoomRequest])
        .out(jsonBody[RoomResponse])

    val delete =
      base.delete
        .in(basePath / path[UUID]("id"))
        .out(statusCode(StatusCode.NoContent))

    val all = List(list, getById, create, patch, delete)
  }

  // ---- Bookings (cross-building; flat) ------------------------------------------------------

  object bookings {

    val list =
      base.get
        .in("api" / "v1" / "bookings")
        .in(bookingFilterInput)
        .out(jsonBody[List[BookingResponse]])

    val getById =
      base.get
        .in("api" / "v1" / "bookings" / path[UUID]("id"))
        .out(jsonBody[BookingResponse])

    val create =
      base.post
        .in("api" / "v1" / "bookings")
        .in(jsonBody[CreateBookingRequest])
        .out(statusCode(StatusCode.Created) and jsonBody[BookingResponse])

    val delete =
      base.delete
        .in("api" / "v1" / "bookings" / path[UUID]("id"))
        .out(statusCode(StatusCode.NoContent))

    val all = List(list, getById, create, delete)
  }

  // ---- Cross-resource search ----------------------------------------------------------------

  object search {

    /**
     * GET /api/v1/search/rooms — rooms within a radius of `(nearLat, nearLon)` matching the supplied room criteria
     * and, if a date range is given, free during it. Backed by an `INNER JOIN rooms × buildings` with `ST_DWithin`
     * on the building's geometry; results carry the parent building's id / name / location inline.
     */
    val rooms =
      base.get
        .in("api" / "v1" / "search" / "rooms")
        .in(roomSearchInput)
        .out(jsonBody[List[RoomWithBuildingResponse]])

    /**
     * GET /api/v1/search/availability — buildings within a radius, with the count of free rooms in `[from, to)`,
     * ordered by free-room count descending. Designed for a "find a meeting room" landing page.
     */
    val availability =
      base.get
        .in("api" / "v1" / "search" / "availability")
        .in(availabilityInput)
        .out(jsonBody[List[BuildingAvailabilityResponse]])

    val all = List(rooms, availability)
  }

  lazy val all: List[sttp.tapir.AnyEndpoint] =
    buildings.all ++ rooms.all ++ bookings.all ++ search.all
}
