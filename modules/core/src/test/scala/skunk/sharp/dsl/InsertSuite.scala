/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  private val tasks = Table.of[Task]("tasks").withPrimary("id")

  test("insert renders column list and placeholders") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "write docs", priority = 1, due = Option.empty[OffsetDateTime]))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4)"""
    )
  }

  test("insert.returning single column") {
    val id = UUID.randomUUID
    val af = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returning(t => t.id)
      .compile.af

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
      .compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"""
    )
  }

  test("insert.returningAll returns the whole row") {
    val id = UUID.randomUUID
    val af = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returningAll
      .compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) RETURNING "id", "title", "priority", "due""""
    )
  }

  test("insert.onConflictDoNothing appends ON CONFLICT DO NOTHING") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflictDoNothing
      .compile.af
    assert(af.fragment.sql.endsWith(" ON CONFLICT DO NOTHING"))
  }

  test("insert.onConflict(col).doNothing targets a specific column") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflict(t => t.id)
      .doNothing
      .compile.af
    assert(af.fragment.sql.endsWith("""ON CONFLICT ("id") DO NOTHING"""))
  }

  test("insert.onConflict(col).doUpdateFromExcluded references excluded.<col>") {
    val af = tasks
      .insert((id = UUID.randomUUID, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .onConflict(t => t.id)
      .doUpdateFromExcluded((t, ex) => (t.title := ex.title, t.priority := ex.priority))
      .compile.af

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
      .compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO UPDATE SET "title" = $5, "priority" = $6"""
    )
  }

  test("insert can omit defaulted columns (sequence PK / NOW() timestamp)") {
    val tasksWithDefaults = tasks.withDefault("id").withDefault("due")
    val af                = tasksWithDefaults
      .insert((title = "x", priority = 1))
      .compile.af

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
    val af = tasks.insert.values(rows).compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"""
    )
  }

  test("insert.values takes a cats.Reducible (NonEmptyVector)") {
    val rows = NonEmptyVector.of(
      (id = UUID.randomUUID, title = "a", priority = 1, due = Option.empty[OffsetDateTime])
    )
    val af = tasks.insert.values(rows).compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4)"""
    )
  }

  test("insert accepts a case class instance (full row via Mirror.ProductOf)") {
    val row = Task(UUID.randomUUID, "write docs", 1, Option.empty[OffsetDateTime])
    val af  = tasks.insert(row).compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4)"""
    )
  }

  test("insert accepts a case class that is a subset of the table (defaulted columns omitted)") {
    case class TaskInput(title: String, priority: Int)
    val tasksWithDefaults = tasks.withDefault("id").withDefault("due")
    val af                = tasksWithDefaults.insert(TaskInput("x", 1)).compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("title", "priority") VALUES ($1, $2)"""
    )
  }

  test("insert.values batches case-class rows through cats.Reducible") {
    val rows = NonEmptyList.of(
      Task(UUID.randomUUID, "a", 1, Option.empty[OffsetDateTime]),
      Task(UUID.randomUUID, "b", 2, Option.empty[OffsetDateTime])
    )
    val af = tasks.insert.values(rows).compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"""
    )
  }

  test("insert.returningTuple multiple columns") {
    val id = UUID.randomUUID
    val af = tasks
      .insert((id = id, title = "x", priority = 1, due = Option.empty[OffsetDateTime]))
      .returningTuple(t => (t.id, t.priority))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """INSERT INTO "tasks" ("id", "title", "priority", "due") VALUES ($1, $2, $3, $4) RETURNING "id", "priority""""
    )
  }
}
