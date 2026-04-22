package skunk.sharp.example.api

import io.circe.Codec
import sttp.tapir.Schema

import java.time.{LocalDate, OffsetDateTime}
import java.util.UUID

case class RoomResponse(id: UUID, name: String, capacity: Int) derives Codec.AsObject, Schema
case class CreateRoomRequest(name: String, capacity: Int) derives Codec.AsObject, Schema
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
