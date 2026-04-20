package skunk.sharp

import skunk.data.Type

import java.time.OffsetDateTime
import java.util.UUID

object TableOfSuite {
  case class User(id: UUID, email: String, createdAt: OffsetDateTime, deletedAt: Option[OffsetDateTime])
}

class TableOfSuite extends munit.FunSuite {
  import TableOfSuite.User

  test("Table.of derives columns from a case class Mirror; tpe is the skunk Type from the codec") {
    val users = Table.of[User]("users")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "createdAt", "deletedAt"))
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.text, Type.timestamptz, Type.timestamptz))
    assertEquals(cols.map(_.isNullable), List(false, false, false, true))
    assertEquals(cols.map(_.hasDefault), List(false, false, false, false))
  }

  test("withPrimary / withUnique update runtime metadata") {
    val users = Table.of[User]("users").withPrimary("id").withUnique("email")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.name).zip(cols.map(_.isPrimary)),
      List("id" -> true, "email" -> false, "createdAt" -> false, "deletedAt" -> false)
    )
    assertEquals(
      cols.map(_.name).zip(cols.map(_.isUnique)),
      List("id" -> false, "email" -> true, "createdAt" -> false, "deletedAt" -> false)
    )
  }

  test("withDefault flips runtime hasDefault flag and advances the Default phantom") {
    val users = Table.of[User]("users").withDefault("createdAt")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.name).zip(cols.map(_.hasDefault)),
      List("id" -> false, "email" -> false, "createdAt" -> true, "deletedAt" -> false)
    )

    summon[ColumnHasDefault[users.columns.type, "createdAt"] =:= true]
    summon[ColumnHasDefault[users.columns.type, "id"] =:= false]
  }
}

/** Local helper: checks whether the column named `N` in `Cols` has the `ColumnAttr.Default` marker. */
type ColumnHasDefault[Cols <: Tuple, N <: String & Singleton] = skunk.sharp.ColumnDefault[Cols, N]
