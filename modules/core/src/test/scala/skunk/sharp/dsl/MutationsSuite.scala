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

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object MutationsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class MutationsSuite extends munit.FunSuite {
  import MutationsSuite.User

  private val users = Table.of[User]("users")

  test("update with single SET and WHERE") {
    val af = users.update
      .set(u => u.email := "new@example.com")
      .where(u => u.id === UUID.fromString("00000000-0000-0000-0000-000000000001"))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2"""
    )
  }

  test("update with multiple SETs (tuple form)") {
    val af = users.update
      .set(u => (u.email := "x", u.age := 42))
      .where(u => u.id === UUID.fromString("00000000-0000-0000-0000-000000000001"))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3"""
    )
  }

  test("delete with WHERE") {
    val af = users.delete.where(u => u.email === "gone@example.com").compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "email" = $1""")
  }

  test(".deleteAll explicitly opts into an unconditional DELETE") {
    val af = users.delete.deleteAll.compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users"""")
  }

  test(".updateAll explicitly opts into an unconditional UPDATE") {
    val af = users.update.set(u => u.age := 0).updateAll.compile.af
    assertEquals(af.fragment.sql, """UPDATE "users" SET "age" = $1""")
  }

  test("delete without .where or .deleteAll does not compile") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.delete.compile
    """)
    assert(err.nonEmpty, "expected compile error: .compile only exists after .where or .deleteAll")
  }

  test("update without .where or .updateAll does not compile") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.update.set(u => u.age := 0).compile
    """)
    assert(err.nonEmpty, "expected compile error: .compile only exists after .where or .updateAll")
  }

  test("update .returning appends RETURNING <col>") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.email := "x")
      .where(u => u.id === id)
      .returning(u => u.id)
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2 RETURNING "id""""
    )
  }

  test("update .returningTuple returns multiple columns") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.age := 42)
      .where(u => u.id === id)
      .returningTuple(u => (u.id, u.age))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "age""""
    )
  }

  test("update .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update.set(u => u.age := 42).where(u => u.id === id).returningAll.compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }

  test("delete .returning") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === id).returning(u => u.email).compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "id" = $1 RETURNING "email"""")
  }

  test("delete .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === id).returningAll.compile.af
    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" WHERE "id" = $1 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }
}
