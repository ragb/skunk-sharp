package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.sharp.*
import skunk.sharp.contrib.citext.Citext
import skunk.sharp.contrib.pgtrgm.*
import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.example.domain.BookingRow

import java.time.LocalDate
import java.util.UUID

trait BookingRepository {
  def findFiltered(filters: List[BookingFilter]): Kleisli[Stream[IO, *], Session[IO], BookingRow]
  def findById(id: UUID): Kleisli[IO, Session[IO], Option[BookingRow]]
  def findByRoom(roomId: UUID): Kleisli[Stream[IO, *], Session[IO], BookingRow]
  def findOverlapping(roomId: UUID, period: PgRange[LocalDate]): Kleisli[IO, Session[IO], List[BookingRow]]
  def create(data: BookingRow.Create): Kleisli[IO, Session[IO], UUID]
  def delete(id: UUID): Kleisli[IO, Session[IO], Unit]
}

object BookingRepository {

  val live: BookingRepository = new BookingRepository {
    private val t = BookingRow.table

    /** Captured columns view of the bookings table — see RoomRepository's `cv` for the rationale. */
    private val cv = t.columnsView

    private val selectRow =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]

    // Compiled once — Args = Void.
    private val findAllQ = selectRow.compile

    // Simple statements stay positional; named Params are for the ones where order is easy to get wrong.

    // Compiled once — Args = UUID.
    private val findByIdQ =
      selectRow.where(b => b.id === Param[UUID]).compile

    // Compiled once — Args = UUID.
    private val findByRoomQ =
      selectRow.where(b => b.room_id === Param[UUID]).compile

    // Compiled once — Args = (UUID, PgRange[LocalDate]).
    private val findOverlappingQ =
      selectRow
        .where(b => b.room_id === Param[UUID] && b.period.overlaps(Param[PgRange[LocalDate]]))
        .compile

    // Compiled once — named (four fields, two of them text): run with (roomId = …, bookerName = …, title = …, period = …).
    private val createQ =
      t.insert
        .withParams((
          room_id = Param.named["roomId", UUID],
          booker_name = Param.named["bookerName", Citext],
          title = Param.named["title", String],
          period = Param.named["period", PgRange[LocalDate]]
        ))
        .returning(b => b.id)
        .compile

    // Compiled once — Args = UUID.
    private val deleteQ =
      t.delete.where(b => b.id === Param[UUID]).compile

    /**
     * Translate one filter case to a `Where[Void]`. The half-bounded ranges for "starts on or after" / "ends on or
     * before" use `<@` (containedBy) against an open-ended probe range — that lets Postgres use the GiST index on
     * `period` if one exists.
     */
    private def toWhere(f: BookingFilter): Where[skunk.Void] = f match {
      case BookingFilter.RoomsIn(ids) =>
        cv.room_id.in(ids.map(Param.bind(_)))

      case BookingFilter.BookerNameContains(s) =>
        // ILIKE on a citext column is equivalent to LIKE — both are case-insensitive at storage level. Kept as
        // ILIKE to make the case-insensitive intent obvious at the call site.
        cv.booker_name.ilike(Param.bind(s"%$s%"))

      case BookingFilter.BookerNameSimilar(q) =>
        // Trigram-similarity match — catches typos / partials a substring LIKE would miss. Backed by the GIN index
        // from the V2 migration. The threshold comes from Postgres's session-level pg_trgm.similarity_threshold.
        cv.booker_name.similarTrgm(Param.bind(q))

      case BookingFilter.TitleContains(s) =>
        cv.title.ilike(Param.bind(s"%$s%"))

      case BookingFilter.OverlapsPeriod(period) =>
        cv.period.overlaps(Param.bind(period))

      case BookingFilter.StartsOnOrAfter(date) =>
        cv.period.containedBy(Param.bind(PgRange[LocalDate](lower = Some(date))))

      case BookingFilter.EndsOnOrBefore(date) =>
        cv.period.containedBy(Param.bind(PgRange[LocalDate](upper = Some(date), upperInclusive = true)))
    }

    def findFiltered(filters: List[BookingFilter]): Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      if filters.isEmpty then findAllQ.streamKF[IO]()
      else
        selectRow.where(_ => allOf(filters.map(toWhere)*))
          .compile.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[BookingRow]] =
      findByIdQ.optionK[IO](id)

    def findByRoom(roomId: UUID): Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      findByRoomQ.streamKF[IO](roomId, 64)

    def findOverlapping(roomId: UUID, period: PgRange[LocalDate]): Kleisli[IO, Session[IO], List[BookingRow]] =
      findOverlappingQ.runK[IO]((roomId, period))

    def create(data: BookingRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((
        roomId = data.room_id,
        bookerName = data.booker_name,
        title = data.title,
        period = data.period
      ))
    // Note: data.booker_name is already a Citext (set by the transformer at the request boundary).

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO](id).void
  }

}
