package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object JoinSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

class JoinSuite extends munit.FunSuite {
  import JoinSuite.*

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")

  test("INNER JOIN renders FROM … AS … INNER JOIN … AS … ON …") {
    val af = users
      .alias("u")
      .innerJoin(posts.alias("p"))
      .on(r => r.u.id ==== r.p.user_id)
      .select(r => (r.u.email, r.p.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "u"."email", "p"."title" FROM "users" AS "u" INNER JOIN "posts" AS "p" ON "u"."id" = "p"."user_id""""
    )
  }

  test("INNER JOIN with WHERE + ORDER BY + LIMIT") {
    val af = users
      .alias("u")
      .innerJoin(posts.alias("p"))
      .on(r => r.u.id ==== r.p.user_id)
      .select(r => (r.u.email, r.p.title, r.p.created_at))
      .where(r => r.u.age >= 18)
      .orderBy(r => r.p.created_at.desc)
      .limit(10)
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "u"."email", "p"."title", "p"."created_at" FROM "users" AS "u" INNER JOIN "posts" AS "p" ON "u"."id" = "p"."user_id" WHERE "u"."age" >= $1 ORDER BY "p"."created_at" DESC LIMIT 10"""
    )
  }

  test("LEFT JOIN renders LEFT JOIN and ON still sees declared types") {
    val af = users
      .alias("u")
      .leftJoin(posts.alias("p"))
      .on(r => r.u.id ==== r.p.user_id) // right side not yet nullabilified in ON
      .select(r => (r.u.email, r.p.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "u"."email", "p"."title" FROM "users" AS "u" LEFT JOIN "posts" AS "p" ON "u"."id" = "p"."user_id""""
    )
  }

  test("LEFT JOIN — right-side columns decode as Option in .select") {
    // r.p.title has type TypedColumn[Option[String], true] here, so the compiled query returns Option[String].
    val _: CompiledQuery[Option[String]] = users
      .alias("u")
      .leftJoin(posts.alias("p"))
      .on(r => r.u.id ==== r.p.user_id)
      .select(r => r.p.title)
      .compile
  }

  test("aliased aggregate through a JOIN projection") {
    val af = users
      .alias("u")
      .innerJoin(posts.alias("p"))
      .on(r => r.u.id ==== r.p.user_id)
      .select(r => (r.u.email, Pg.count(r.p.id).as("post_count")))
      .compile
      .af

    assert(
      af.fragment.sql.contains("""count("p"."id") AS "post_count""""),
      af.fragment.sql
    )
  }

  test(".compile without .on does not exist (IncompleteJoin2 has no .compile)") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[JoinSuite.User]("users")
      val posts = Table.of[JoinSuite.Post]("posts")
      users.alias("u").innerJoin(posts.alias("p")).compile
    """)
    assert(errs.nonEmpty, "expected a compile error: .on is required before .compile")
  }
}
