package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object UpdateFromDeleteUsingSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
  case class Tag(id: UUID, post_id: UUID, name: String)
}

class UpdateFromDeleteUsingSuite extends munit.FunSuite {
  import UpdateFromDeleteUsingSuite.*

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")
  private val tags  = Table.of[Tag]("tags")

  test("UPDATE … FROM one extra source renders correct SQL") {
    val af = users.update
      .from(posts)
      .set(r => r.users.age := 1)
      .where(r => r.users.id ==== r.posts.user_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 FROM "posts" WHERE "users"."id" = "posts"."user_id""""
    )
  }

  test("UPDATE … FROM two extra sources renders both in FROM list") {
    val af = users.update
      .from(posts)
      .from(tags)
      .set(r => r.users.age := 1)
      .where(r => r.users.id ==== r.posts.user_id && r.posts.id ==== r.tags.post_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 FROM "posts", "tags" WHERE ("users"."id" = "posts"."user_id" AND "posts"."id" = "tags"."post_id")"""
    )
  }

  test("UPDATE … FROM with RETURNING renders correctly") {
    val af = users.update
      .from(posts)
      .set(r => r.users.age := 1)
      .where(r => r.users.id ==== r.posts.user_id)
      .returning(r => r.users.email)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 FROM "posts" WHERE "users"."id" = "posts"."user_id" RETURNING "users"."email""""
    )
  }

  test("UPDATE … FROM .updateAll renders without WHERE") {
    val af = users.update
      .from(posts)
      .set(r => r.users.age := 1)
      .updateAll
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 FROM "posts""""
    )
  }

  test("UPDATE … FROM with aliased source uses alias in FROM clause") {
    val af = users.update
      .from(posts.alias("p"))
      .set(r => r.users.age := 1)
      .where(r => r.users.id ==== r.p.user_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 FROM "posts" AS "p" WHERE "users"."id" = "p"."user_id""""
    )
  }

  test("DELETE … USING one extra source renders correct SQL") {
    val af = users.delete
      .using(posts)
      .where(r => r.users.id ==== r.posts.user_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" USING "posts" WHERE "users"."id" = "posts"."user_id""""
    )
  }

  test("DELETE … USING two extra sources renders both in USING list") {
    val af = users.delete
      .using(posts)
      .using(tags)
      .where(r => r.users.id ==== r.posts.user_id && r.posts.id ==== r.tags.post_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" USING "posts", "tags" WHERE ("users"."id" = "posts"."user_id" AND "posts"."id" = "tags"."post_id")"""
    )
  }

  test("DELETE … USING with RETURNING renders correctly") {
    val af = users.delete
      .using(posts)
      .where(r => r.users.id ==== r.posts.user_id)
      .returning(r => r.users.email)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" USING "posts" WHERE "users"."id" = "posts"."user_id" RETURNING "users"."email""""
    )
  }

  test("DELETE … USING .deleteAll renders without WHERE") {
    val af = users.delete
      .using(posts)
      .deleteAll
      .compile.af

    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" USING "posts""""
    )
  }

  test("DELETE … USING with aliased source uses alias in USING clause") {
    val af = users.delete
      .using(posts.alias("p"))
      .where(r => r.users.id ==== r.p.user_id)
      .compile.af

    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" USING "posts" AS "p" WHERE "users"."id" = "p"."user_id""""
    )
  }

  test("single-table update still compiles — Name on UpdateBuilder is backward-compatible") {
    val af = users.update
      .set(u => u.age := 1)
      .where(u => u.id === UUID.randomUUID())
      .compile.af

    assert(af.fragment.sql.startsWith("""UPDATE "users" SET "age" = """))
  }

  test("single-table delete still compiles — Name on DeleteBuilder is backward-compatible") {
    val af = users.delete
      .where(u => u.id === UUID.randomUUID())
      .compile.af

    assert(af.fragment.sql.startsWith("""DELETE FROM "users" WHERE """))
  }
}
