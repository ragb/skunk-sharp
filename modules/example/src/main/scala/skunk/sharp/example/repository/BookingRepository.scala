package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.example.domain.BookingRow

import java.time.LocalDate
import java.util.UUID

trait BookingRepository {
  def findAll: Kleisli[Stream[IO, *], Session[IO], BookingRow]
  def findById(id: UUID): Kleisli[IO, Session[IO], Option[BookingRow]]
  def findByRoom(roomId: UUID): Kleisli[Stream[IO, *], Session[IO], BookingRow]
  def findOverlapping(roomId: UUID, start: LocalDate, end: LocalDate): Kleisli[IO, Session[IO], List[BookingRow]]
  def create(data: BookingRow.Create): Kleisli[IO, Session[IO], UUID]
  def delete(id: UUID): Kleisli[IO, Session[IO], Unit]
}

object BookingRepository {

  val live: BookingRepository = new BookingRepository {
    private val t = BookingRow.table

    private val selectRow =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]

    // Compiled once — Args = Void.
    private val findAllQ = selectRow.compile

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

    // Compiled once — Args = (UUID, String, String, PgRange[LocalDate]) matching Create's field order.
    private val createQ =
      t.insert
        .withParams((
          room_id     = Param[UUID],
          booker_name = Param[String],
          title       = Param[String],
          period      = Param[PgRange[LocalDate]]
        ))
        .returning(b => b.id)
        .compile

    // Compiled once — Args = UUID.
    private val deleteQ =
      t.delete.where(b => b.id === Param[UUID]).compile

    def findAll: Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      findAllQ.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[BookingRow]] =
      findByIdQ.optionK[IO](id)

    def findByRoom(roomId: UUID): Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      findByRoomQ.streamKF[IO](roomId, 64)

    def findOverlapping(roomId: UUID, start: LocalDate, end: LocalDate): Kleisli[IO, Session[IO], List[BookingRow]] =
      findOverlappingQ.runK[IO]((roomId, PgRange[LocalDate](lower = Some(start), upper = Some(end))))

    def create(data: BookingRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((data.room_id, data.booker_name, data.title, data.period))

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO](id).void
  }

}
