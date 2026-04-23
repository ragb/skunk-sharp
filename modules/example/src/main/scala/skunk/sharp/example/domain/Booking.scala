package skunk.sharp.example.domain

import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.PgRange

import java.time.{LocalDate, OffsetDateTime}
import java.util.UUID

case class BookingRow(
  id: UUID,
  room_id: UUID,
  booker_name: String,
  title: String,
  period: PgRange[LocalDate],
  created_at: OffsetDateTime
)

object BookingRow {
  val table = Table.of[BookingRow]("bookings").withPrimary("id").withDefault("id").withDefault("created_at")

  case class Create(room_id: UUID, booker_name: String, title: String, period: PgRange[LocalDate])
}
