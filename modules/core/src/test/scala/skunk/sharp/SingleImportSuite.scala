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

package skunk.sharp

// Deliberately the ONLY import a DSL user needs. If anything breaks, the package object is incomplete.
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object SingleImportSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class SingleImportSuite extends munit.FunSuite {
  import SingleImportSuite.User

  private val users = Table.of[User]("users").withPrimary("id")

  test("read, filter, project, order, limit — all from one import") {
    val af = users.select
      .where(u => u.age >= 18 && u.email.like("%@example.com") && u.deleted_at.isNull)
      .orderBy(u => u.created_at.desc)
      .limit(10)
      .apply(u => (u.id, Pg.lower(u.email)))
      .compile.af

    assert(af.fragment.sql.startsWith("""SELECT "id", lower("email") FROM "users""""), clue = af.fragment.sql)
  }

  test("insert single + batch") {
    val row = (
      id = UUID.randomUUID,
      email = "x@y",
      age = 1,
      created_at = OffsetDateTime.now(),
      deleted_at = Option.empty[OffsetDateTime]
    )
    val af1 = users.insert(row).compile.af
    val af2 = users.insert.values(row, row).compile.af
    assert(af1.fragment.sql.startsWith("""INSERT INTO "users""""), clue = af1.fragment.sql)
    assert(af2.fragment.sql.contains(" VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)"), clue = af2.fragment.sql)
  }

  test("update + delete") {
    val id  = UUID.randomUUID
    val af1 = users.update.set(u => u.email := "new").where(u => u.id === id).compile.af
    val af2 = users.delete.where(u => u.id === id).compile.af
    assert(af1.fragment.sql.startsWith("""UPDATE "users" SET "email" = $1"""), clue = af1.fragment.sql)
    assertEquals(af2.fragment.sql, """DELETE FROM "users" WHERE "id" = $1""")
  }
}
