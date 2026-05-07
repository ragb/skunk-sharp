package skunk.sharp.example.api

import cats.data.NonEmptyList
import cats.syntax.all.*
import io.github.arainko.ducktape.*
import skunk.sharp.data.Range
import skunk.sharp.example.domain.{BookingRow, RoomRow}
import skunk.sharp.example.repository.{BookingFilter, RoomFilter}
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

  extension (req: CreateRoomRequest)
    def toRow: RoomRow.Create = req.to[RoomRow.Create]

  extension (req: PatchRoomRequest)
    def toRow: RoomRow.Patch = req.to[RoomRow.Patch]

  extension (req: CreateBookingRequest)

    def toRow: BookingRow.Create =
      req.into[BookingRow.Create]
        .transform(
          Field.renamed(_.room_id, _.roomId),
          Field.renamed(_.booker_name, _.bookerName),
          Field.computed(_.period, r => PgRange[LocalDate](lower = Some(r.startDate), upper = Some(r.endDate)))
        )

  extension (q: RoomFilterQuery)

    /**
     * Project the query DTO onto the repository's `RoomFilter` ADT — absent fields disappear, present fields become one
     * filter case each. Multi-value fields turn into IN-style cases via `NonEmptyList`.
     */
    def toFilters: List[RoomFilter] = List(
      q.minCapacity.map(RoomFilter.CapacityAtLeast(_)),
      q.maxCapacity.map(RoomFilter.CapacityAtMost(_)),
      q.nameContains.map(RoomFilter.NameContains(_)),
      NonEmptyList.fromList(q.names).map(RoomFilter.NamesIn(_)),
      NonEmptyList.fromList(q.ids).map(RoomFilter.IdsIn(_))
    ).flatten

  extension (q: BookingFilterQuery)

    /**
     * Project the booking query DTO onto `BookingFilter`. `overlapsFrom`/`overlapsTo` are paired — the `OverlapsPeriod`
     * filter is only emitted when both bounds are present.
     */
    def toFilters: List[BookingFilter] = {
      val overlap = (q.overlapsFrom, q.overlapsTo).tupled.map { case (f, t) =>
        BookingFilter.OverlapsPeriod(f, t)
      }
      List(
        NonEmptyList.fromList(q.roomIds).map(BookingFilter.RoomsIn(_)),
        q.bookerNameContains.map(BookingFilter.BookerNameContains(_)),
        q.titleContains.map(BookingFilter.TitleContains(_)),
        overlap,
        q.startsOnOrAfter.map(BookingFilter.StartsOnOrAfter(_)),
        q.endsOnOrBefore.map(BookingFilter.EndsOnOrBefore(_))
      ).flatten
    }

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
