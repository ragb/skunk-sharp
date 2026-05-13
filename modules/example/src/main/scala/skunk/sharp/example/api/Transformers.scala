package skunk.sharp.example.api

import cats.data.NonEmptyList
import cats.syntax.all.*
import io.github.arainko.ducktape.*
import skunk.sharp.contrib.citext.Citext
import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.data.Range
import skunk.sharp.example.domain.{BookingRow, RoomRow}
import skunk.sharp.example.repository.{BookingFilter, RoomFilter}
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate

object Transformers {

  extension (row: RoomRow)

    def toResponse: RoomResponse =
      row.into[RoomResponse]
        .transform(
          // LTree <: String, so Scala-side it's already assignable; spell the conversion out so the
          // wire shape (plain String) is obvious at the boundary.
          Field.computed(_.location, r => r.location: String),
          Field.computed(_.amenities, r => amenitiesToWire(r.amenities))
        )

  extension (row: BookingRow)

    def toResponse: BookingResponse =
      row.into[BookingResponse]
        .transform(
          Field.renamed(_.roomId, _.room_id),
          // Citext <: String — passes through unchanged on the wire.
          Field.computed(_.bookerName, b => b.booker_name: String),
          Field.renamed(_.createdAt, _.created_at),
          Field.computed(_.startDate, b => rangeStart(b.period)),
          Field.computed(_.endDate, b => rangeEnd(b.period))
        )

  extension (req: CreateRoomRequest)

    def toRow: RoomRow.Create =
      req.into[RoomRow.Create]
        .transform(
          Field.computed(_.location, r => LTree(r.location)),
          Field.computed(_.amenities, r => amenitiesFromWire(r.amenities))
        )

  extension (req: PatchRoomRequest)
    def toRow: RoomRow.Patch = req.to[RoomRow.Patch]

  extension (req: CreateBookingRequest)

    def toRow: BookingRow.Create =
      req.into[BookingRow.Create]
        .transform(
          Field.renamed(_.room_id, _.roomId),
          Field.computed(_.booker_name, r => Citext(r.bookerName)),
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
      NonEmptyList.fromList(q.ids).map(RoomFilter.IdsIn(_)),
      q.locationUnder.map(s => RoomFilter.LocationUnder(LTree(s))),
      q.hasAmenity.map(RoomFilter.HasAmenity(_))
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
        q.bookerNameSimilar.map(BookingFilter.BookerNameSimilar(_)),
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

  /**
   * Project an `Hstore` (whose values are `Option[String]`) to the wire shape — drop entries whose value is NULL so the
   * JSON object only carries the present keys. Lossy by design: the wire model is "the room has these stated
   * amenities" rather than "the database has these keys, some with NULL values".
   */
  private def amenitiesToWire(h: Hstore): Map[String, String] =
    h.collect { case (k, Some(v)) => k -> v }.toMap

  private def amenitiesFromWire(m: Map[String, String]): Hstore =
    Hstore(m.view.mapValues(v => Some(v): Option[String]).toMap)

}
