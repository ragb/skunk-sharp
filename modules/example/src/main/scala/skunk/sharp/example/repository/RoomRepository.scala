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
  def findFiltered(filters: List[RoomFilter]): Kleisli[Stream[IO, *], Session[IO], RoomRow]
  def findById(id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]]
  def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID]
  def patch(id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]]
  def delete(id: UUID): Kleisli[IO, Session[IO], Unit]
}

/**
 * Static-template repository — every query that has a fixed shape is compiled exactly once at object construction.
 * Calls bind parameters and run; nothing is re-built per request.
 *
 * Two methods earn an exception: `.patch` (variable SET list per call) and `.findFiltered` (variable WHERE shape driven
 * by a runtime `List[RoomFilter]`). Both compile a fresh query per call and bake values via `Param.bind`, so the
 * user-facing `Args` collapses to `Void` and there's nothing to thread at execute time.
 */
object RoomRepository {

  val live: RoomRepository = new RoomRepository {
    private val t = RoomRow.table

    /**
     * Captured columns view, statically typed as `ColumnsView[<RoomRow.table.Cols>]` — `cv.id` / `cv.name` /
     * `cv.capacity` resolve via the named-tuple selector. Kept on the impl so [[toWhere]] is a normal method (not
     * nested inside a `where(c => …)` lambda). Safe for the unaliased single-source case used here.
     */
    private val cv = t.columnsView

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

    /**
     * Translate one filter case to a `Where[Void]`. Every arm bakes its runtime value via `Param.bind`, so the result
     * has `Args = Void` and can be AND-folded with `dsl.allOf`.
     */
    private def toWhere(f: RoomFilter): Where[skunk.Void] = f match {
      case RoomFilter.CapacityAtLeast(n) => cv.capacity >= Param.bind(n)
      case RoomFilter.CapacityAtMost(n)  => cv.capacity <= Param.bind(n)
      case RoomFilter.NameContains(s)    => cv.name.ilike(Param.bind(s"%$s%"))
      case RoomFilter.NamesIn(ns)        => cv.name.in(ns.map(Param.bind(_)))
      case RoomFilter.IdsIn(ids)         => cv.id.in(ids.map(Param.bind(_)))
    }

    def findFiltered(filters: List[RoomFilter]): Kleisli[Stream[IO, *], Session[IO], RoomRow] =
      if filters.isEmpty then findAllQ.streamKF[IO]() // hit the static-cache fast path
      else
        // `allOf` AND-folds the per-filter Wheres; empty would have rendered `WHERE TRUE` but we
        // short-circuit above to keep the static cache.
        selectRow.where(_ => allOf(filters.map(toWhere)*))
          .compile.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]] =
      findByIdQ.optionK[IO](id)

    def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((data.name, data.capacity))

    /**
     * `.patch` builds a different SET list per call depending on which fields are `Some`. There is no single static SQL
     * that covers every subset of N optional fields, so this method stays on the captured-args path: each call compiles
     * a fresh `CommandTemplate` shaped to the present fields and Param.bind-bakes the values. See the "When captured
     * args still earn their keep" note below.
     */
    def patch(id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]] =
      if (data.name.isEmpty && data.capacity.isEmpty) findById(id)
      else
        t.update
          .patch(data)
          .where(r => r.id === Param.bind(id))
          .returningAll.to[RoomRow]
          .compile.optionK[IO]

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO](id).void
  }

}
