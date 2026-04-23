package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.sharp.dsl.*
import skunk.sharp.example.domain.RoomRow

import java.util.UUID

trait RoomRepository {
  def findAll: Kleisli[Stream[IO, *], Session[IO], RoomRow]
  def findById(id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]]
  def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID]
  def patch(id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]]
  def delete(id: UUID): Kleisli[IO, Session[IO], Unit]
}

object RoomRepository {

  val live: RoomRepository = new RoomRepository {
    private val t = RoomRow.table

    private def selectRow = t.select(r => (r.id, r.name, r.capacity)).to[RoomRow]

    def findAll: Kleisli[Stream[IO, *], Session[IO], RoomRow] =
      selectRow.compile.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]] =
      selectRow.where(r => r.id === id).compile.optionK[IO]

    def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID] =
      t.insert(data).returning(r => r.id).compile.uniqueK[IO]

    def patch(id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]] =
      if (data.name.isEmpty && data.capacity.isEmpty) findById(id)
      else
        t.update
          .patch(data)
          .where(r => r.id === id)
          .returningAll.to[RoomRow]
          .compile.optionK[IO]

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      t.delete.where(r => r.id === id).compile.runK[IO].void
  }

}
