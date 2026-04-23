package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
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

    private def selectRow =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]

    def findAll: Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      selectRow.compile.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[BookingRow]] =
      selectRow.where(b => b.id === id).compile.optionK[IO]

    def findByRoom(roomId: UUID): Kleisli[Stream[IO, *], Session[IO], BookingRow] =
      selectRow.where(b => b.room_id === roomId).compile.streamKF[IO]()

    def findOverlapping(roomId: UUID, start: LocalDate, end: LocalDate): Kleisli[IO, Session[IO], List[BookingRow]] = {
      val probe = PgRange[LocalDate](lower = Some(start), upper = Some(end))
      selectRow
        .where(b => b.room_id === roomId && b.period.overlaps(param(probe)))
        .compile.runK[IO]
    }

    def create(data: BookingRow.Create): Kleisli[IO, Session[IO], UUID] =
      t.insert(data).returning(b => b.id).compile.uniqueK[IO]

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      t.delete.where(b => b.id === id).compile.runK[IO].void
  }

}
