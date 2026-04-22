package skunk.sharp.example.api

import io.scalaland.chimney.dsl.*
import skunk.sharp.data.Range
import skunk.sharp.example.domain.{BookingRow, RoomRow}
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate

object Transformers {

  extension (row: RoomRow)
    def toResponse: RoomResponse = row.transformInto[RoomResponse]

  extension (row: BookingRow)
    def toResponse: BookingResponse =
      row
        .into[BookingResponse]
        .withFieldRenamed(_.room_id, _.roomId)
        .withFieldRenamed(_.booker_name, _.bookerName)
        .withFieldRenamed(_.created_at, _.createdAt)
        .withFieldComputed(_.startDate, b => rangeStart(b.period))
        .withFieldComputed(_.endDate, b => rangeEnd(b.period))
        .transform

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

}
