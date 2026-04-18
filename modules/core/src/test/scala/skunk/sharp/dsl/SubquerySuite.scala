package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object SubquerySuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

class SubquerySuite extends munit.FunSuite {
  import SubquerySuite.*

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")

  // ---- Uncorrelated subqueries — inner builders, no `.compile` at the inner level --------------

  test("uncorrelated scalar subquery as a projection value") {
    val af = users
      .select(u => (u.email, posts.select(_ => Pg.countAll).asExpr))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "email", (SELECT count(*) FROM "posts") FROM "users""""
    )
  }

  test("uncorrelated .in(subquery)") {
    val af = users.select
      .where(u => u.id.in(posts.select(p => p.user_id)))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age" FROM "users" WHERE "id" IN (SELECT "user_id" FROM "posts")"""
    )
  }

  test("uncorrelated EXISTS — no .compile, no Where(…) wrap") {
    val af = users.select
      .where(_ => Pg.exists(posts.select))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age" FROM "users" WHERE EXISTS (SELECT "id", "user_id", "title", "created_at" FROM "posts")"""
    )
  }

  // ---- Correlated (inner built inside outer lambda, closes over outer column) ------------------

  test("correlated scalar subquery — per-user post count in projection") {
    val af = users
      .alias("u")
      .select(u =>
        (
          u.email,
          posts.select(_ => Pg.countAll).where(p => p.user_id ==== u.id).asExpr
        )
      )
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "u"."email", (SELECT count(*) FROM "posts" WHERE "user_id" = "u"."id") FROM "users" AS "u""""
    )
  }

  test("correlated EXISTS — users that have at least one post") {
    val af = users
      .alias("u")
      .select(u => u.email)
      .where(u => Pg.exists(posts.select(_ => TypedExpr.lit(1)).where(p => p.user_id ==== u.id)))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "u"."email" FROM "users" AS "u" WHERE EXISTS (SELECT $1 FROM "posts" WHERE "user_id" = "u"."id")"""
    )
  }

  test("correlated WHERE combining AND with EXISTS") {
    val af = users
      .alias("u")
      .select(u => u.email)
      .where(u => u.age >= 18 && Pg.exists(posts.select(_ => TypedExpr.lit(1)).where(p => p.user_id ==== u.id)))
      .compile
      .af

    assert(
      af.fragment.sql.contains("""WHERE ("u"."age" >= $1 AND EXISTS (SELECT $2 FROM "posts" WHERE "user_id" = "u"."id"))"""),
      af.fragment.sql
    )
  }
}
