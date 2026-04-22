package skunk.sharp.example.api

import sttp.tapir.*
import sttp.tapir.json.circe.*
import sttp.model.StatusCode

import java.util.UUID

object Endpoints {

  // Error type is (StatusCode, ApiError) for all endpoints.
  // Server logic returns Left((StatusCode.NotFound, ApiError("..."))) for 404 etc.
  private val base = endpoint.errorOut(statusCode and jsonBody[ApiError])

  object rooms {

    val list =
      base.get.in("api" / "v1" / "rooms").out(jsonBody[List[RoomResponse]])

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

    val list =
      base.get.in("api" / "v1" / "bookings").out(jsonBody[List[BookingResponse]])

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
