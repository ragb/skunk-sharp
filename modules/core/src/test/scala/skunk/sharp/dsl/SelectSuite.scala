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

object SelectSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class SelectSuite extends munit.FunSuite {
  import SelectSuite.User

  private val users = Table.of[User]("users")

  test("select.from emits whole-row SELECT with quoted identifiers") {
    val af = users.select.compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users""""
    )
  }

  test("where appends a WHERE clause") {
    val af = users.select.where(u => u.email === "a@b").compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users" WHERE "email" = $1"""
    )
  }

  test("chained .where combines with AND") {
    val af = users.select
      .where(u => u.age >= 18)
      .where(u => u.deleted_at.isNull)
      .compile.af
    assert(af.fragment.sql.contains("""WHERE ("age" >= $1 AND "deleted_at" IS NULL)"""))
  }

  test("limit and offset appear after WHERE") {
    val af = users.select
      .where(u => u.age >= 18)
      .limit(10)
      .offset(5)
      .compile.af
    assert(af.fragment.sql.endsWith(" LIMIT 10 OFFSET 5"))
  }

  test("order by single column") {
    val af = users.select.orderBy(u => u.created_at.desc).compile.af
    assert(af.fragment.sql.endsWith(""" ORDER BY "created_at" DESC"""))
  }

  test("order by multiple columns") {
    val af = users.select.orderBy(u => (u.age.asc, u.email.asc)).compile.af
    assert(af.fragment.sql.endsWith(""" ORDER BY "age" ASC, "email" ASC"""))
  }

  test("order by nulls first / nulls last") {
    val af1 = users.select.orderBy(u => u.deleted_at.desc.nullsLast).compile.af
    assert(af1.fragment.sql.endsWith(""" ORDER BY "deleted_at" DESC NULLS LAST"""), af1.fragment.sql)

    val af2 = users.select.orderBy(u => u.deleted_at.asc.nullsFirst).compile.af
    assert(af2.fragment.sql.endsWith(""" ORDER BY "deleted_at" ASC NULLS FIRST"""), af2.fragment.sql)
  }

  test("project single column via .apply") {
    val af = users.select(u => u.email).compile.af
    assertEquals(af.fragment.sql, """SELECT "email" FROM "users"""")
  }

  test("project with a function call") {
    val af = users.select(u => Pg.lower(u.email)).compile.af
    assertEquals(af.fragment.sql, """SELECT lower("email") FROM "users"""")
  }

  test("tuple projection via .apply yields a multi-column SELECT") {
    val af = users.select(u => (u.email, u.age)).compile.af
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test("named-tuple projection input compiles (labels used at call site only)") {
    val af = users.select(u => (email = u.email, age = u.age)).compile.af
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test(".to[CaseClass] maps projection rows into a case class") {
    case class Snapshot(email: String, age: Int)
    val af = users.select(u => (u.email, u.age)).to[Snapshot].compile.af
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test("distinctRows renders SELECT DISTINCT") {
    val af = users.select.distinctRows.apply(u => u.age).compile.af
    assertEquals(af.fragment.sql, """SELECT DISTINCT "age" FROM "users"""")
  }

  test("empty.select(…) renders a FROM-less query") {
    val af1 = empty.select(_ => Pg.now).compile.af
    assertEquals(af1.fragment.sql, "SELECT now()")

    val af2 = empty.select(_ => (Pg.now, Pg.currentDate)).compile.af
    assertEquals(af2.fragment.sql, "SELECT now(), current_date")
  }

  test("FOR UPDATE appears after WHERE / ORDER BY / LIMIT / OFFSET") {
    val af = users.select.where(u => u.age >= 18).orderBy(u => u.id.asc).limit(5).forUpdate.compile.af
    assert(af.fragment.sql.endsWith(" FOR UPDATE"), clue = af.fragment.sql)
  }

  test("FOR UPDATE SKIP LOCKED") {
    val af = users.select.forUpdate.skipLocked.compile.af
    assert(af.fragment.sql.endsWith(" FOR UPDATE SKIP LOCKED"), clue = af.fragment.sql)
  }

  test("FOR UPDATE NOWAIT") {
    val af = users.select.forUpdate.noWait.compile.af
    assert(af.fragment.sql.endsWith(" FOR UPDATE NOWAIT"), clue = af.fragment.sql)
  }

  test("FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE") {
    assert(users.select.forShare.compile.af.fragment.sql.endsWith(" FOR SHARE"))
    assert(users.select.forNoKeyUpdate.compile.af.fragment.sql.endsWith(" FOR NO KEY UPDATE"))
    assert(users.select.forKeyShare.compile.af.fragment.sql.endsWith(" FOR KEY SHARE"))
  }

  test("locking carries through to ProjectedSelect") {
    val af = users.select.forUpdate.skipLocked.apply(u => u.id).compile.af
    assert(af.fragment.sql.endsWith(" FOR UPDATE SKIP LOCKED"), clue = af.fragment.sql)
  }

  test(".forUpdate on a View does not compile (Postgres rejects FOR UPDATE on a view)") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      case class ActiveUser(id: java.util.UUID, email: String)
      val v = View.of[ActiveUser]("active_users")
      v.select.forUpdate
    """)
    assert(err.nonEmpty, "expected compile error: forUpdate is Table-only")
  }

  test(".forShare on a View does not compile") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      case class ActiveUser(id: java.util.UUID, email: String)
      val v = View.of[ActiveUser]("active_users")
      v.select.forShare
    """)
    assert(errs.nonEmpty)
  }

  test(".skipLocked on a View does not compile (even attempted via a prior attempted forUpdate)") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      case class ActiveUser(id: java.util.UUID, email: String)
      val v = View.of[ActiveUser]("active_users")
      v.select.skipLocked
    """)
    assert(errs.nonEmpty)
  }

}
