package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.util.UUID

object SelectRelationSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String)
}

class SelectRelationSuite extends munit.FunSuite {
  import SelectRelationSuite.*

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")

  test(".asRelation emits `(<inner>) AS \"<alias>\"` in FROM — single outer .compile") {
    val active = users.select.where(u => u.age >= 18).asRelation("active")
    val af     = active.select.compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age" FROM (SELECT "id", "email", "age" FROM "users" WHERE "age" >= $1) AS "active""""
    )
  }

  test("derived relation joins a base table — outer .compile walks both sources") {
    val active = users.select.where(u => u.age >= 18).asRelation("active")
    val af     = active
      .innerJoin(posts)
      .on(r => r.active.id ==== r.posts.user_id)
      .select(r => (r.active.email, r.posts.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "active"."email", "posts"."title" FROM (SELECT "id", "email", "age" FROM "users" WHERE "age" >= $1) AS "active" INNER JOIN "posts" ON "active"."id" = "posts"."user_id""""
    )
  }

  test("inner parameters flow into the outer-query argument list in declaration order") {
    val active = users.select.where(u => u.age >= 18).asRelation("active")
    val af     = active.select.where(a => a.email === "x@y.z").compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age" FROM (SELECT "id", "email", "age" FROM "users" WHERE "age" >= $1) AS "active" WHERE "email" = $2"""
    )
  }
}
