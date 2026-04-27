package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.sharp.*
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

/**
 * Static-template repository — every query that has a fixed shape is compiled exactly once at object
 * construction. Calls bind parameters and run; nothing is re-built per request.
 *
 * `.patch` is the lone exception: see [[live.patch]] for why it must stay on captured-args.
 */
object RoomRepository {

  val live: RoomRepository = new RoomRepository {
    private val t = RoomRow.table

    private val selectRow =
      t.select(r => (r.id, r.name, r.capacity)).to[RoomRow]

    // Compiled once — Args = Void, R = RoomRow.
    private val findAllQ = selectRow.compile

    // Compiled once — Args = UUID.
    private val findByIdQ =
      selectRow.where(r => r.id === Param[UUID]).compile

    // Compiled once — Args = (String, Int) (Create's fields, in declaration order).
    private val createQ =
      t.insert
        .withParams((name = Param[String], capacity = Param[Int]))
        .returning(r => r.id)
        .compile

    // Compiled once — Args = UUID.
    private val deleteQ =
      t.delete.where(r => r.id === Param[UUID]).compile

    def findAll: Kleisli[Stream[IO, *], Session[IO], RoomRow] =
      findAllQ.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]] =
      findByIdQ.optionK[IO](id)

    def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((data.name, data.capacity))

    /**
     * `.patch` builds a different SET list per call depending on which fields are `Some`. There is no
     * single static SQL that covers every subset of N optional fields, so this method stays on the
     * captured-args path: each call compiles a fresh `CommandTemplate` shaped to the present fields and
     * Param.bind-bakes the values. See the "When captured args still earn their keep" note below.
     */
    def patch(id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]] =
      if (data.name.isEmpty && data.capacity.isEmpty) findById(id)
      else
        t.update
          .patch(data)
          .where(r => r.id === id)
          .returningAll.to[RoomRow]
          .compile.optionK[IO]

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO](id).void
  }

}
