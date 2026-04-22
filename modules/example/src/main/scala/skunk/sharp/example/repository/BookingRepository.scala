package skunk.sharp.example.repository

import cats.effect.IO
import skunk.Session
import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.example.domain.BookingRow

import java.time.LocalDate
import java.util.UUID

trait BookingRepository {
  def findAll(s: Session[IO]): IO[List[BookingRow]]
  def findById(id: UUID, s: Session[IO]): IO[Option[BookingRow]]
  def findByRoom(roomId: UUID, s: Session[IO]): IO[List[BookingRow]]
  def findOverlapping(roomId: UUID, start: LocalDate, end: LocalDate, s: Session[IO]): IO[List[BookingRow]]
  def create(roomId: UUID, bookerName: String, title: String, start: LocalDate, end: LocalDate, s: Session[IO]): IO[UUID]
  def delete(id: UUID, s: Session[IO]): IO[Boolean]
}

object BookingRepository {

  val live: BookingRepository = new BookingRepository {
    private val t = BookingRow.table

    private def selectAll(s: Session[IO]): IO[List[BookingRow]] =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]
        .compile.run(s)

    def findAll(s: Session[IO]): IO[List[BookingRow]] = selectAll(s)

    def findById(id: UUID, s: Session[IO]): IO[Option[BookingRow]] =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]
        .where(b => b.id === id)
        .compile.option(s)

    def findByRoom(roomId: UUID, s: Session[IO]): IO[List[BookingRow]] =
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]
        .where(b => b.room_id === roomId)
        .compile.run(s)

    def findOverlapping(roomId: UUID, start: LocalDate, end: LocalDate, s: Session[IO]): IO[List[BookingRow]] = {
      val probe = PgRange[LocalDate](lower = Some(start), upper = Some(end))
      t.select(b => (b.id, b.room_id, b.booker_name, b.title, b.period, b.created_at))
        .to[BookingRow]
        .where(b => b.room_id === roomId && b.period.overlaps(param(probe)))
        .compile.run(s)
    }

    def create(roomId: UUID, bookerName: String, title: String, start: LocalDate, end: LocalDate, s: Session[IO]): IO[UUID] = {
      val period = PgRange[LocalDate](lower = Some(start), upper = Some(end))
      t.insert((room_id = roomId, booker_name = bookerName, title = title, period = period))
        .returning(b => b.id)
        .compile.unique(s)
    }

    def delete(id: UUID, s: Session[IO]): IO[Boolean] =
      t.delete.where(b => b.id === id).compile.run(s).as(true)
  }

}
