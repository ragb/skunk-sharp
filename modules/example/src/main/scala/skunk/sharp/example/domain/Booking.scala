package skunk.sharp.example.domain

import skunk.sharp.contrib.citext.Citext
import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.PgRange

import java.time.{LocalDate, OffsetDateTime}
import java.util.UUID

/**
 * A booking. `booker_name` is `citext` so case-insensitive equality / ILIKE / trigram search all behave correctly
 * without per-call `lower(...)` ceremony, and a single index on the column serves both exact lookups and the trigram
 * GIN index from the V2 migration.
 */
case class BookingRow(
  id: UUID,
  room_id: UUID,
  booker_name: Citext,
  title: String,
  period: PgRange[LocalDate],
  created_at: OffsetDateTime
)

object BookingRow {
  val table = Table.of[BookingRow]("bookings").withPrimary("id").withDefault("id").withDefault("created_at")

  case class Create(room_id: UUID, booker_name: Citext, title: String, period: PgRange[LocalDate])
}
