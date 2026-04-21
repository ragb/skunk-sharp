package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object DistinctOnAnyAllSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
  case class Event(id: UUID, user_id: UUID, action: String, created_at: OffsetDateTime)
}

class DistinctOnAnyAllSuite extends munit.FunSuite {
  import DistinctOnAnyAllSuite.*

  private val users  = Table.of[User]("users")
  private val events = Table.of[Event]("events")

  // ---- DISTINCT ON --------------------------------------------------------------------------------

  test("distinctOn on a whole-row SelectBuilder renders SELECT DISTINCT ON (…)") {
    val af = events.select
      .distinctOn(e => e.user_id)
      .orderBy(e => (e.user_id.asc, e.created_at.desc))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT DISTINCT ON ("user_id") "id", "user_id", "action", "created_at" FROM "events" ORDER BY "user_id" ASC, "created_at" DESC"""
    )
  }

  test("distinctOn with multiple columns — comma-joined inside the parens") {
    val af = events.select
      .distinctOn(e => (e.user_id, e.action))
      .orderBy(e => (e.user_id.asc, e.action.asc, e.created_at.desc))
      .compile
      .af

    assert(
      af.fragment.sql.startsWith("""SELECT DISTINCT ON ("user_id", "action") """),
      af.fragment.sql
    )
  }

  test("distinctOn carries across .select(f) — projection-form, with an expression DISTINCT ON target") {
    val af = events.select
      .distinctOn(e => e.user_id)
      .select(e => (e.user_id, e.action))
      .orderBy(e => (e.user_id.asc, e.created_at.desc))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT DISTINCT ON ("user_id") "user_id", "action" FROM "events" ORDER BY "user_id" ASC, "created_at" DESC"""
    )
  }

  test("distinctOn wins over distinctRows when both are set") {
    val af = events.select
      .distinctRows
      .distinctOn(e => e.user_id)
      .compile
      .af

    assert(af.fragment.sql.startsWith("""SELECT DISTINCT ON ("user_id") """), af.fragment.sql)
    assert(!af.fragment.sql.contains("DISTINCT DISTINCT"), af.fragment.sql)
  }

  // ---- ANY / ALL over a subquery ----------------------------------------------------------------

  test("ltAny renders `< ANY (<subquery>)`") {
    val inner = users.select(x => x.age).where(x => x.email.like("%@x"))
    val af    = users.select(u => u.email).where(u => u.age.ltAny(inner)).compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "email" FROM "users" WHERE "age" < ANY (SELECT "age" FROM "users" WHERE "email" LIKE $1)"""
    )
  }

  test("gteAll renders `>= ALL (<subquery>)`") {
    val inner = users.select(x => x.age)
    val af    = users.select(u => u.email).where(u => u.age.gteAll(inner)).compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "email" FROM "users" WHERE "age" >= ALL (SELECT "age" FROM "users")"""
    )
  }

  test("ANY / ALL accept a ProjectedSelect subquery (single-column projection)") {
    val inner = users.select(x => x.age).where(x => x.email.like("admin-%"))
    val af    = users.select(u => u.email).where(u => u.age.gtAll(inner)).compile.af

    assert(af.fragment.sql.contains("""> ALL ("""), af.fragment.sql)
    assert(af.fragment.sql.contains("""SELECT "age" FROM "users""""), af.fragment.sql)
  }

  // ---- OVERLAPS -----------------------------------------------------------------------------------

  test("Pg.overlaps renders the (a, b) OVERLAPS (c, d) shape") {
    val tsA = lit(42) // placeholder; real use is with time-typed expressions (below)
    val af  = users.select(u => u.email).where(u => Pg.overlaps(u.created_at, u.created_at, u.created_at, u.created_at))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "email" FROM "users" WHERE ("created_at", "created_at") OVERLAPS ("created_at", "created_at")"""
    )
    val _ = tsA // unused
  }

  test("Pg.overlaps with two parameterised time bounds") {
    val t1 = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    val t2 = OffsetDateTime.parse("2020-12-31T00:00:00Z")
    val af = users.select(u => u.email)
      .where(u => Pg.overlaps(u.created_at, u.created_at, param(t1), param(t2)))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "email" FROM "users" WHERE ("created_at", "created_at") OVERLAPS ($1, $2)"""
    )
  }
}
