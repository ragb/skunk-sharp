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
    val (af, _) = users.select.compile
    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users""""
    )
  }

  test("where appends a WHERE clause") {
    val (af, _) = users.select.where(u => u.email === "a@b").compile
    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users" WHERE "email" = $1"""
    )
  }

  test("chained .where combines with AND") {
    val (af, _) = users.select
      .where(u => u.age >= 18)
      .where(u => u.deleted_at.isNull)
      .compile
    assert(af.fragment.sql.contains("""WHERE ("age" >= $1 AND "deleted_at" IS NULL)"""))
  }

  test("limit and offset appear after WHERE") {
    val (af, _) = users.select
      .where(u => u.age >= 18)
      .limit(10)
      .offset(5)
      .compile
    assert(af.fragment.sql.endsWith(" LIMIT 10 OFFSET 5"))
  }

  test("order by single column") {
    val (af, _) = users.select.orderBy(u => u.created_at.desc).compile
    assert(af.fragment.sql.endsWith(""" ORDER BY "created_at" DESC"""))
  }

  test("order by multiple columns") {
    val (af, _) = users.select.orderBy(u => (u.age.asc, u.email.asc)).compile
    assert(af.fragment.sql.endsWith(""" ORDER BY "age" ASC, "email" ASC"""))
  }

  test("project single column via .apply") {
    val (af, _) = users.select(u => u.email).compile
    assertEquals(af.fragment.sql, """SELECT "email" FROM "users"""")
  }

  test("project with a function call") {
    val (af, _) = users.select(u => Pg.lower(u.email)).compile
    assertEquals(af.fragment.sql, """SELECT lower("email") FROM "users"""")
  }

  test("tuple projection via .apply yields a multi-column SELECT") {
    val (af, _) = users.select(u => (u.email, u.age)).compile
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test("named-tuple projection input compiles (labels used at call site only)") {
    val (af, _) = users.select(u => (email = u.email, age = u.age)).compile
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test(".as[CaseClass] maps projection rows into a case class") {
    case class Snapshot(email: String, age: Int)
    val (af, _) = users.select(u => (u.email, u.age)).as[Snapshot].compile
    assertEquals(af.fragment.sql, """SELECT "email", "age" FROM "users"""")
  }

  test("distinctRows renders SELECT DISTINCT") {
    val (af, _) = users.select.distinctRows.apply(u => u.age).compile
    assertEquals(af.fragment.sql, """SELECT DISTINCT "age" FROM "users"""")
  }

  test("empty.select(…) renders a FROM-less query") {
    val (af1, _) = empty.select(_ => Pg.now).compile
    assertEquals(af1.fragment.sql, "SELECT now()")

    val (af2, _) = empty.select(_ => (Pg.now, Pg.currentDate)).compile
    assertEquals(af2.fragment.sql, "SELECT now(), current_date")
  }

  test("FOR UPDATE appears after WHERE / ORDER BY / LIMIT / OFFSET") {
    val (af, _) = users.select.where(u => u.age >= 18).orderBy(u => u.id.asc).limit(5).forUpdate.compile
    assert(af.fragment.sql.endsWith(" FOR UPDATE"), clue = af.fragment.sql)
  }

  test("FOR UPDATE SKIP LOCKED") {
    val (af, _) = users.select.forUpdate.skipLocked.compile
    assert(af.fragment.sql.endsWith(" FOR UPDATE SKIP LOCKED"), clue = af.fragment.sql)
  }

  test("FOR UPDATE NOWAIT") {
    val (af, _) = users.select.forUpdate.noWait.compile
    assert(af.fragment.sql.endsWith(" FOR UPDATE NOWAIT"), clue = af.fragment.sql)
  }

  test("FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE") {
    assert(users.select.forShare.compile._1.fragment.sql.endsWith(" FOR SHARE"))
    assert(users.select.forNoKeyUpdate.compile._1.fragment.sql.endsWith(" FOR NO KEY UPDATE"))
    assert(users.select.forKeyShare.compile._1.fragment.sql.endsWith(" FOR KEY SHARE"))
  }

  test("locking carries through to ProjectedSelect") {
    val (af, _) = users.select.forUpdate.skipLocked.apply(u => u.id).compile
    assert(af.fragment.sql.endsWith(" FOR UPDATE SKIP LOCKED"), clue = af.fragment.sql)
  }
}
