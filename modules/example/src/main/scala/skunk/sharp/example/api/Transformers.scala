package skunk.sharp.example.api

import io.github.arainko.ducktape.*
import skunk.sharp.data.Range
import skunk.sharp.example.domain.{BookingRow, RoomRow}
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate

object Transformers {

  extension (row: RoomRow)
    def toResponse: RoomResponse = row.to[RoomResponse]

  extension (row: BookingRow)
    def toResponse: BookingResponse =
      row.into[BookingResponse]
        .transform(
          Field.renamed(_.roomId, _.room_id),
          Field.renamed(_.bookerName, _.booker_name),
          Field.renamed(_.createdAt, _.created_at),
          Field.computed(_.startDate, b => rangeStart(b.period)),
          Field.computed(_.endDate, b => rangeEnd(b.period))
        )

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
