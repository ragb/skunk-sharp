package skunk.sharp

import skunk.codec.all.*
import skunk.data.Type

import java.time.OffsetDateTime
import java.util.UUID

class TableBuilderSuite extends munit.FunSuite {

  test("builder accumulates columns with correct metadata; tpe is the codec's skunk Type") {
    val users =
      Table
        .builder("users")
        .column("id", uuid, primary = true)
        .column("email", varchar(256), unique = true)
        .columnDefaulted("created_at", timestamptz)
        .columnOpt("deleted_at", timestamptz)
        .build

    assertEquals(users.name, "users")
    assertEquals(users.schema, Option.empty[String])

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.size, 4)
    assertEquals(cols.map(_.name), List("id", "email", "created_at", "deleted_at"))
    assertEquals(
      cols.map(_.tpe),
      List(Type.uuid, Type.varchar(256), Type.timestamptz, Type.timestamptz)
    )
    assertEquals(cols.map(_.isNullable), List(false, false, false, true))
    assertEquals(cols.map(_.hasDefault), List(false, false, true, false))
    assertEquals(cols.map(_.isPrimary), List(true, false, false, false))
    assertEquals(cols.map(_.isUnique), List(false, true, false, false))
  }

  test("HasColumn is true for declared columns, false otherwise") {
    val t = Table.builder("t").column("a", int4).column("b", text).build
    summon[HasColumn[t.columns.type, "a"] =:= true]
    summon[HasColumn[t.columns.type, "b"] =:= true]
    summon[HasColumn[t.columns.type, "c"] =:= false]
  }

  test("qualifiedName quotes identifiers") {
    val t1 = Table.builder("users").column("id", int4).build
    assertEquals(t1.qualifiedName, "\"users\"")

    val t2 = Table.builder("users").column("id", int4).build.copy(schema = Some("public"))
    assertEquals(t2.qualifiedName, "\"public\".\"users\"")
  }

  test("NamedRowOf exposes the column shape as a named tuple") {
    val users = Table
      .builder("users")
      .column("id", uuid)
      .column("email", text)
      .columnOpt("deleted_at", timestamptz)
      .build

    summon[NamedRowOf[users.columns.type] =:= (id: UUID, email: String, deleted_at: Option[OffsetDateTime])]
  }
}
