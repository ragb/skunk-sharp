package skunk.sharp.example.domain

import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.dsl.*

import java.util.UUID

/**
 * A meeting room. The `location` ltree models physical hierarchy (`acme.dublin.floor3.wing_east`) so the API can offer
 * filters like "every room on Dublin floor 3" via a single `path <@ prefix` query. `amenities` is an hstore of
 * free-form key/value metadata (projector resolution, A/V kit, …) — denser than jsonb for flat key/value lookups and
 * filterable with `?` (hasKey) / `@>` (contains).
 */
case class RoomRow(id: UUID, name: String, capacity: Int, location: LTree, amenities: Hstore)

object RoomRow {

  // `location` and `amenities` carry DB defaults (V2 migration), so they can be omitted from inserts that don't care
  // to set them — the typed `Create` payload below requires them, but `.withDefault` is what lets us model that.
  val table = Table.of[RoomRow]("rooms")
    .withPrimary("id")
    .withDefault("id")
    .withDefault("location")
    .withDefault("amenities")

  case class Create(name: String, capacity: Int, location: LTree, amenities: Hstore)
  case class Patch(name: Option[String], capacity: Option[Int])
}
