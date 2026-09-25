package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.sharp.*
import skunk.sharp.contrib.hstore.*
import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.ltree.*
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.dsl.{*, given}
import skunk.sharp.example.domain.{BookingRow, RoomRow}

import java.util.UUID

/**
 * Room operations are always **scoped to a building**: every method takes the `buildingId` from the URL path and the
 * generated SQL carries a `building_id = $1` clause that pairs with the user filters. This mirrors the REST layout
 * (`/api/v1/buildings/{buildingId}/rooms/…`) and means a request that targets the wrong building (room belongs to a
 * different one) cleanly returns 404 instead of leaking cross-tenant rows.
 */
trait RoomRepository {
  def findFiltered(buildingId: UUID, filters: List[RoomFilter]): Kleisli[Stream[IO, *], Session[IO], RoomRow]
  def findById(buildingId: UUID, id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]]
  def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID]
  def patch(buildingId: UUID, id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]]
  def delete(buildingId: UUID, id: UUID): Kleisli[IO, Session[IO], Unit]

  /**
   * Make the building's rooms match `rooms` (by name) in one statement: update changed capacities, insert new rooms,
   * delete rooms missing from the list — except rooms that still have bookings, which are kept. Returns one
   * `merge_action()` (`INSERT` / `UPDATE` / `DELETE`) per affected room.
   */
  def sync(buildingId: UUID, rooms: List[RoomRow.Sync]): Kleisli[IO, Session[IO], List[String]]
}

/**
 * Static-template repository — every query that has a fixed shape is compiled exactly once at object construction.
 * Calls bind parameters and run; nothing is re-built per request.
 *
 * Two methods earn an exception: `.patch` (variable SET list per call) and `.findFiltered` (variable WHERE shape driven
 * by a runtime `List[RoomFilter]` plus the building scope). Both compile a fresh query per call and bake values via
 * `Param.bind`, so the user-facing `Args` collapses to `Void` and there's nothing to thread at execute time.
 */
object RoomRepository {

  val live: RoomRepository = new RoomRepository {
    private val t  = RoomRow.table
    private val cv = t.columnsView

    private val selectRow =
      t.select(r => (r.id, r.building_id, r.name, r.capacity, r.location, r.amenities)).to[RoomRow]

    // Compiled once — Args = UUID (the building id).
    private val findAllInBuildingQ =
      selectRow.where(r => r.building_id === Param[UUID]).compile

    // Compiled once — Args = (UUID, UUID) (building id, room id).
    private val findByIdQ =
      selectRow.where(r => r.building_id === Param[UUID] && r.id === Param[UUID]).compile

    // Compiled once — Args = (UUID, String, Int, LTree, Hstore) (Create's fields, in declaration order).
    private val createQ =
      t.insert
        .withParams((
          building_id = Param[UUID],
          name = Param[String],
          capacity = Param[Int],
          location = Param[LTree],
          amenities = Param[Hstore]
        ))
        .returning(r => r.id)
        .compile

    // Compiled once — a single MERGE whose whole batch is one typed parameter: `Pg.unnestRows` splits the
    // `List[RoomRow.Sync]` into `unnest($1, $2)` arrays at encode time, so the same prepared statement serves any number
    // of rooms. Args = (List[RoomRow.Sync], UUID, UUID, UUID): the rooms, then the building id for the ON scope, the
    // INSERT, and the BY SOURCE scope.
    private val syncQ: QueryTemplate[(List[RoomRow.Sync], UUID, UUID, UUID), String] =
      t.merge(Pg.unnestRows[RoomRow.Sync].alias("incoming"))
        .on(r => r.rooms.building_id === Param[UUID] && r.rooms.name === r.incoming.name)
        .whenMatched(r => (r.rooms.capacity !== r.incoming.capacity))
        .update(r => r.rooms.capacity := r.incoming.capacity)
        .whenNotMatched
        .insert(i => (building_id = Param[UUID], name = i.name, capacity = i.capacity))
        // Every room outside this building is also "not matched by source" (the ON clause pins the building), so the
        // DELETE is scoped to the building explicitly — and skips rooms that still have bookings.
        .whenNotMatchedBySource(r =>
          r.building_id === Param[UUID] &&
            Pg.notExists(BookingRow.table.select(_ => lit(1)).where(b => b.room_id === r.id))
        )
        .delete
        .returning(_ => Pg.mergeAction)

    // Compiled once — Args = (UUID, UUID).
    private val deleteQ =
      t.delete.where(r => r.building_id === Param[UUID] && r.id === Param[UUID]).compile

    /** Translate one user filter to a `Where[Void]`. The building-id scope is added on top in [[findFiltered]]. */
    private def toWhere(f: RoomFilter): Where[skunk.Void] = f match {
      case RoomFilter.CapacityAtLeast(n)    => cv.capacity >= Param.bind(n)
      case RoomFilter.CapacityAtMost(n)     => cv.capacity <= Param.bind(n)
      case RoomFilter.NameContains(s)       => cv.name.ilike(Param.bind(s"%$s%"))
      case RoomFilter.NamesIn(ns)           => cv.name.in(ns.map(Param.bind(_)))
      case RoomFilter.IdsIn(ids)            => cv.id.in(ids.map(Param.bind(_)))
      case RoomFilter.LocationUnder(prefix) =>
        cv.location.isDescendantOf(Param.bind(prefix))
      case RoomFilter.HasAmenity(key) =>
        cv.amenities.hasKey(Param.bind(key))
    }

    def findFiltered(
      buildingId: UUID,
      filters: List[RoomFilter]
    ): Kleisli[Stream[IO, *], Session[IO], RoomRow] =
      if filters.isEmpty then findAllInBuildingQ.streamKF[IO](buildingId, 64)
      else {
        // AND-fold the per-filter Wheres on top of the `building_id = $1` scope.
        val scoped: List[Where[skunk.Void]] = (cv.building_id === Param.bind(buildingId)) :: filters.map(toWhere)
        selectRow.where(_ => allOf(scoped*)).compile.streamKF[IO]()
      }

    def findById(buildingId: UUID, id: UUID): Kleisli[IO, Session[IO], Option[RoomRow]] =
      findByIdQ.optionK[IO]((buildingId, id))

    def create(data: RoomRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((data.building_id, data.name, data.capacity, data.location, data.amenities))

    /**
     * `.patch` builds a different SET list per call depending on which fields are `Some`. There is no single static SQL
     * that covers every subset of N optional fields, so this method stays on the captured-args path: each call compiles
     * a fresh `CommandTemplate` shaped to the present fields and Param.bind-bakes the values.
     */
    def patch(buildingId: UUID, id: UUID, data: RoomRow.Patch): Kleisli[IO, Session[IO], Option[RoomRow]] =
      if (data.name.isEmpty && data.capacity.isEmpty) findById(buildingId, id)
      else
        t.update
          .patch(data)
          .where(r => r.building_id === Param.bind(buildingId) && r.id === Param.bind(id))
          .returningAll.to[RoomRow]
          .compile.optionK[IO]

    def delete(buildingId: UUID, id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO]((buildingId, id)).void

    def sync(buildingId: UUID, rooms: List[RoomRow.Sync]): Kleisli[IO, Session[IO], List[String]] =
      syncQ.runK[IO]((rooms, buildingId, buildingId, buildingId))
  }

}
