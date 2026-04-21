package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object JoinSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
  case class Tag(id: UUID, post_id: UUID, name: String)
}

class JoinSuite extends munit.FunSuite {
  import JoinSuite.*

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")
  private val tags  = Table.of[Tag]("tags")

  test("INNER JOIN (no explicit alias) — table names used as aliases") {
    val af = users
      .innerJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id""""
    )
  }

  test("INNER JOIN with WHERE + ORDER BY + LIMIT (no explicit alias)") {
    val af = users
      .innerJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title, r.posts.created_at))
      .where(r => r.users.age >= 18)
      .orderBy(r => r.posts.created_at.desc)
      .limit(10)
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title", "posts"."created_at" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "users"."age" >= $1 ORDER BY "posts"."created_at" DESC LIMIT 10"""
    )
  }

  test("LEFT JOIN renders LEFT JOIN; ON still sees declared types") {
    val af = users
      .leftJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id) // right side not yet nullabilified in ON
      .select(r => (r.users.email, r.posts.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" LEFT JOIN "posts" ON "users"."id" = "posts"."user_id""""
    )
  }

  test("LEFT JOIN — right-side columns decode as Option in .select") {
    // r.posts.title has type TypedColumn[Option[String], true] here, so the compiled query returns Option[String].
    val _: CompiledQuery[Option[String]] = users
      .leftJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => r.posts.title)
      .compile
  }

  test("aliased aggregate through a JOIN projection") {
    val af = users
      .innerJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, Pg.count(r.posts.id).as("post_count")))
      .compile
      .af

    assert(
      af.fragment.sql.contains("""count("posts"."id") AS "post_count""""),
      af.fragment.sql
    )
  }

  test("explicit .alias(...) wins over the table-name default") {
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

  test("mixed: auto-aliased left + explicitly-aliased right") {
    val af = users
      .innerJoin(posts.alias("p"))
      .on(r => r.users.id ==== r.p.user_id)
      .select(r => (r.users.email, r.p.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "p"."title" FROM "users" INNER JOIN "posts" AS "p" ON "users"."id" = "p"."user_id""""
    )
  }

  test(".compile without .on does not exist (IncompleteJoin has no .compile)") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[JoinSuite.User]("users")
      val posts = Table.of[JoinSuite.Post]("posts")
      users.innerJoin(posts).compile
    """)
    assert(errs.nonEmpty, "expected a compile error: .on is required before .compile")
  }

  test("three-table INNER JOIN chain") {
    val af = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .innerJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title", "tags"."name" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" INNER JOIN "tags" ON "posts"."id" = "tags"."post_id""""
    )
  }

  test("three-table mixed INNER + LEFT — right-most cols flip to Option") {
    // The compile-time type of the last projection is Option[String] because tags was left-joined.
    val q: CompiledQuery[(String, String, Option[String])] = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .leftJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(
      q.af.fragment.sql.contains("LEFT JOIN \"tags\" ON \"posts\".\"id\" = \"tags\".\"post_id\""),
      q.af.fragment.sql
    )
  }

  test(".crossJoin renders CROSS JOIN; .where supplies the join predicate") {
    val af = users
      .crossJoin(posts)
      .select(r => (r.users.email, r.posts.title))
      .where(r => r.users.id ==== r.posts.user_id)
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" CROSS JOIN "posts" WHERE "users"."id" = "posts"."user_id""""
    )
  }

  test("three-way .crossJoin chain") {
    val af = users
      .crossJoin(posts)
      .crossJoin(tags)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "posts"."title", "tags"."name" FROM "users" CROSS JOIN "posts" CROSS JOIN "tags""""
    )
  }

  // ---- RIGHT JOIN -----------------------------------------------------------------------------

  test("RIGHT JOIN renders RIGHT JOIN; left-side cols decode as Option in .select") {
    // r.users.email is TypedColumn[Option[String], true] because `users` was null-padded by the RIGHT join.
    val q: CompiledQuery[(Option[String], String)] = users
      .rightJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile

    assertEquals(
      q.af.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" RIGHT JOIN "posts" ON "users"."id" = "posts"."user_id""""
    )
  }

  test("RIGHT JOIN: ON sees the LEFT side's declared types (pre-null-padding)") {
    // In .on, left side cols are still non-null — Postgres evaluates ON before null-padding. Project explicitly to
    // force the multi-source compile path and exercise that the ON predicate did type-check against non-null cols.
    val af = users
      .rightJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile.af

    assert(af.fragment.sql.contains("""ON "users"."id" = "posts"."user_id""""), af.fragment.sql)
  }

  // ---- FULL OUTER JOIN ------------------------------------------------------------------------

  test("FULL JOIN renders FULL OUTER JOIN; both sides decode as Option") {
    val q: CompiledQuery[(Option[String], Option[String])] = users
      .fullJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile

    assertEquals(
      q.af.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" FULL OUTER JOIN "posts" ON "users"."id" = "posts"."user_id""""
    )
  }

  // ---- Mixed chains: earlier-source nullability flips on later outer joins --------------------

  test("INNER then RIGHT — the original INNER's cols flip to Option when RIGHT is applied") {
    val q: CompiledQuery[(Option[String], Option[String], String)] = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .rightJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(
      q.af.fragment.sql.contains("""RIGHT JOIN "tags" ON "posts"."id" = "tags"."post_id""""),
      q.af.fragment.sql
    )
  }

  test("LEFT then FULL — LEFT-nullabilified cols stay Option (no double-wrapping) under FULL") {
    // posts was already Option[_] from LEFT. FULL also nullabilifies users. The Scala type must NOT go to
    // Option[Option[_]] for posts — NullableCol is idempotent on already-nullable columns. In the inner ON of the
    // FULL JOIN, posts's cols are already Option (from LEFT) and tags's are still declared (non-null), so we use
    // raw comparisons there by round-tripping through literals that match both sides at the bare types.
    val q: CompiledQuery[(Option[String], Option[String], Option[String])] = users
      .leftJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .fullJoin(tags).on(_ => lit(true))
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(q.af.fragment.sql.contains("FULL OUTER JOIN"), q.af.fragment.sql)
    assert(q.af.fragment.sql.contains("LEFT JOIN"), q.af.fragment.sql)
  }
}
