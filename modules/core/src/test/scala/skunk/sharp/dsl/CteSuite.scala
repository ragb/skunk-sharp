package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object CteSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, status: String)
}

class CteSuite extends munit.FunSuite {
  import CteSuite.{Post, User}

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")

  // ---- whole-row CTEs -------------------------------------------------------------------------

  test("simple whole-row CTE — WITH preamble prepended to SELECT") {
    val active = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val af     = active.select.compile.af
    assertEquals(
      af.fragment.sql,
      """WITH "active_users" AS (SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users" WHERE "deleted_at" IS NULL) SELECT "id", "email", "age", "created_at", "deleted_at" FROM "active_users""""
    )
  }

  test("CTE with additional WHERE on the outer query") {
    val active = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val af     = active.select.where(u => u.age >= 18).compile.af
    assert(af.fragment.sql.startsWith("""WITH "active_users" AS ("""), af.fragment.sql)
    assert(af.fragment.sql.contains("""WHERE "age" >= $"""), af.fragment.sql)
  }

  test("CTE used as projected SELECT source") {
    val active = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val af     = active.select(u => (u.id, u.email)).compile.af
    assertEquals(
      af.fragment.sql,
      """WITH "active_users" AS (SELECT "id", "email", "age", "created_at", "deleted_at" FROM "users" WHERE "deleted_at" IS NULL) SELECT "id", "email" FROM "active_users""""
    )
  }

  // ---- projected CTEs -------------------------------------------------------------------------

  test("projected CTE — named columns from AliasedExpr") {
    val totals = cte(
      "totals",
      users.select(u => (u.age.as("years"), u.email.as("addr")))
    )
    val af = totals.select(t => (t.years, t.addr)).compile.af
    assert(af.fragment.sql.startsWith("""WITH "totals" AS ("""), af.fragment.sql)
    assert(af.fragment.sql.contains("""FROM "totals""""), af.fragment.sql)
  }

  // ---- multiple CTEs in a single query --------------------------------------------------------

  test("two independent CTEs appear in the WITH clause") {
    val activeUsers = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val published   = cte("published", posts.select.where(p => p.status === "published"))
    val af          = activeUsers
      .innerJoin(published).on(r => r.active_users.id ==== r.published.user_id)
      .select(r => (r.active_users.email, r.published.title))
      .compile.af
    assert(af.fragment.sql.startsWith("WITH "), af.fragment.sql)
    assert(af.fragment.sql.contains(""""active_users" AS ("""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""published" AS ("""), af.fragment.sql)
  }

  // ---- chained CTEs ---------------------------------------------------------------------------

  test("chained CTEs — dependency emitted before dependent CTE") {
    val base    = cte("base", users.select.where(u => u.deleted_at.isNull))
    val derived = cte("derived", base.select.where(u => u.age >= 18))
    val af      = derived.select.compile.af
    val sql     = af.fragment.sql
    // "base" must appear before "derived" in the WITH clause
    assert(sql.contains(""""base" AS ("""), sql)
    assert(sql.contains(""""derived" AS ("""), sql)
    assert(sql.indexOf(""""base" AS (""") < sql.indexOf(""""derived" AS ("""), s"'base' should precede 'derived': $sql")
    // Only one WITH keyword — no nesting
    assertEquals(sql.split("WITH ").length - 1, 1, s"expected exactly one WITH: $sql")
  }

  // ---- CTE re-aliased -------------------------------------------------------------------------

  test("CTE re-aliased with .alias") {
    val active = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val af     = active.alias("au").select(u => u.email).compile.af
    assert(af.fragment.sql.contains(""""active_users" AS ("""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""active_users" AS "au""""), af.fragment.sql)
  }

  // ---- type-level checks ----------------------------------------------------------------------

  test("CteRelation is a Relation — compile type is CompiledQuery[NamedRowOf[...]]") {
    val active         = cte("active_users", users.select.where(u => u.deleted_at.isNull))
    val _: Relation[?] = active
  }
}
