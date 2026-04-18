package skunk.sharp.dsl

import cats.data.{NonEmptyList, NonEmptyVector}
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object InsertSuite {
  case class Task(id: UUID, title: String, priority: Int, due: Option[OffsetDateTime])
}

class InsertSuite extends munit.FunSuite {
  import InsertSuite.Task

  private val tasks = Table.of[Task]("tasks")

  test("insert renders column list and placeholders") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "write docs", priority = 1, due = Option.empty[OffsetDateTime]))
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4)"""
    )
  }

  test("insert.returning single column") {
    val id      = UUID.randomUUID
    val (af, _) = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returning(t => t.id)
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) RETURNING "id""""
    )
  }

  test("insert.values batches multiple rows into one VALUES list") {
    val id1 = UUID.randomUUID
    val id2 = UUID.randomUUID
    val af  = tasks.insert
      .values(
        (id = id1, title = "a", priority = 1, due = Option.empty[OffsetDateTime]),
        (id = id2, title = "b", priority = 2, due = Option.empty[OffsetDateTime])
      )
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"""
    )
  }

  test("insert.returningAll returns the whole row") {
    val id      = UUID.randomUUID
    val (af, _) = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returningAll
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) RETURNING "id", "title", "priority", "due""""
    )
  }

  test("insert.onConflictDoNothing appends ON CONFLICT DO NOTHING") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflictDoNothing
      .compile
    assert(af.fragment.sql.endsWith(" ON CONFLICT DO NOTHING"))
  }

  test("insert.onConflict(col).doNothing targets a specific column") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflict(t => t.id)
      .doNothing
      .compile
    assert(af.fragment.sql.endsWith("""ON CONFLICT ("id") DO NOTHING"""))
  }

  test("insert.onConflict(col).doUpdateFromExcluded references excluded.<col>") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflict(t => t.id)
      .doUpdateFromExcluded((t, ex) => (t.title := ex.title, t.priority := ex.priority))
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO UPDATE SET "title" = excluded."title", "priority" = excluded."priority""""
    )
  }

  test("insert.onConflict(col).doUpdate(...) produces a DO UPDATE SET clause") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflict(t => t.id)
      .doUpdate(t => (t.title := "updated", t.priority := 9))
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO UPDATE SET "title" = $5, "priority" = $6"""
    )
  }

  test("insert can omit defaulted columns (sequence PK / NOW() timestamp)") {
    val tasksWithDefaults = tasks.withDefault("id").withDefault("due")
    val af                = tasksWithDefaults
      .insert((title = "x", priority = 1))
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("title", "priority") VALUES ($1, $2)"""
    )
  }

  test("insert rejects a subset that omits a required (non-defaulted) column at compile time") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      val tasks = Table.of[InsertSuite.Task]("tasks")
      tasks.insert((id = UUID.randomUUID))
    """)
    assert(err.nonEmpty, "expected a compile error for missing required columns")
    assert(
      err.exists(_.message.contains("missing required column")),
      s"error message should name the missing column, got: ${err.map(_.message)}"
    )
  }

  test("insert rejects unknown column names at compile time") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      val tasks = Table.of[InsertSuite.Task]("tasks")
      tasks.insert((id = UUID.randomUUID, title = "x", priority = 1, due = None, bogus = "nope"))
    """)
    assert(err.nonEmpty, "expected a compile error for unknown column")
    assert(
      err.exists(_.message.contains("bogus")),
      s"error message should name the bogus column, got: ${err.map(_.message)}"
    )
  }

  test("insert.values takes a cats.Reducible (NonEmptyList)") {
    val rows = NonEmptyList.of(
      (id = UUID.randomUUID, title = "a", priority = 1, due = Option.empty[OffsetDateTime]),
      (id = UUID.randomUUID, title = "b", priority = 2, due = Option.empty[OffsetDateTime])
    )
    val af = tasks.insert.values(rows).compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"""
    )
  }

  test("insert.values takes a cats.Reducible (NonEmptyVector)") {
    val rows = NonEmptyVector.of(
      (id = UUID.randomUUID, title = "a", priority = 1, due = Option.empty[OffsetDateTime])
    )
    val af = tasks.insert.values(rows).compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4)"""
    )
  }

  test("insert.returningTuple multiple columns") {
    val id      = UUID.randomUUID
    val (af, _) = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returningTuple(t => (t.id, t.priority))
      .compile

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) RETURNING "id", "priority""""
    )
  }
}
