package skunk.sharp.example.domain

import skunk.sharp.dsl.*

import java.util.UUID

case class RoomRow(id: UUID, name: String, capacity: Int)

object RoomRow {
  val table = Table.of[RoomRow]("rooms").withPrimary("id").withDefault("id")
}
