package skunk.sharp.example.repository

import cats.effect.IO
import skunk.Session
import skunk.sharp.dsl.*
import skunk.sharp.example.domain.RoomRow

import java.util.UUID

trait RoomRepository {
  def findAll(s: Session[IO]): IO[List[RoomRow]]
  def findById(id: UUID, s: Session[IO]): IO[Option[RoomRow]]
  def create(name: String, capacity: Int, s: Session[IO]): IO[UUID]
  def patch(id: UUID, name: Option[String], capacity: Option[Int], s: Session[IO]): IO[Option[RoomRow]]
  def delete(id: UUID, s: Session[IO]): IO[Boolean]
}

object RoomRepository {

  val live: RoomRepository = new RoomRepository {
    private val t = RoomRow.table

    private def allCols(s: Session[IO]): IO[List[RoomRow]] =
      t.select(r => (r.id, r.name, r.capacity)).to[RoomRow].compile.run(s)

    def findAll(s: Session[IO]): IO[List[RoomRow]] = allCols(s)

    def findById(id: UUID, s: Session[IO]): IO[Option[RoomRow]] =
      t.select(r => (r.id, r.name, r.capacity)).to[RoomRow]
        .where(r => r.id === id)
        .compile.option(s)

    def create(name: String, capacity: Int, s: Session[IO]): IO[UUID] =
      t.insert((name = name, capacity = capacity))
        .returning(r => r.id)
        .compile.unique(s)

    def patch(id: UUID, name: Option[String], capacity: Option[Int], s: Session[IO]): IO[Option[RoomRow]] =
      if (name.isEmpty && capacity.isEmpty) findById(id, s)
      else
        t.update
          .patch((name = name, capacity = capacity))
          .where(r => r.id === id)
          .compile.run(s)
          .flatMap(_ => findById(id, s))

    def delete(id: UUID, s: Session[IO]): IO[Boolean] =
      t.delete.where(r => r.id === id).compile.run(s).as(true)
  }

}
