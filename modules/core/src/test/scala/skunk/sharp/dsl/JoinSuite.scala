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
    val _: QueryTemplate[?, Option[String]] = users
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
    val q: QueryTemplate[?, (String, String, Option[String])] = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .leftJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(
      q.fragment.sql.contains("LEFT JOIN \"tags\" ON \"posts\".\"id\" = \"tags\".\"post_id\""),
      q.fragment.sql
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
    val q: QueryTemplate[?, (Option[String], String)] = users
      .rightJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile

    assertEquals(
      q.fragment.sql,
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
    val q: QueryTemplate[?, (Option[String], Option[String])] = users
      .fullJoin(posts)
      .on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile

    assertEquals(
      q.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" FULL OUTER JOIN "posts" ON "users"."id" = "posts"."user_id""""
    )
  }

  // ---- Mixed chains: earlier-source nullability flips on later outer joins --------------------

  test("INNER then RIGHT — the original INNER's cols flip to Option when RIGHT is applied") {
    val q: QueryTemplate[?, (Option[String], Option[String], String)] = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .rightJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(
      q.fragment.sql.contains("""RIGHT JOIN "tags" ON "posts"."id" = "tags"."post_id""""),
      q.fragment.sql
    )
  }

  // ---- LATERAL joins --------------------------------------------------------------------------
  //
  // LATERAL unlocks correlation in FROM — the inner subquery can reference the outer source's columns directly in
  // its own `.where` / `.limit`. Today's `.alias` only works on whole-row SelectBuilder (projected `.select(f)`
  // doesn't yet support `.alias`), so the inner shape is always `base.select.where(…).limit(N).alias("x")` — the
  // outer query can still project just what it wants from the resulting relation.

  test("INNER JOIN LATERAL renders LATERAL keyword; inner WHERE correlates against outer cols") {
    val af = users
      .innerJoinLateral(u => posts.select.where(p => p.user_id ==== u.id).limit(3).alias("recent"))
      .on(_ => lit(true))
      .select(r => (r.users.email, r.recent.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "recent"."title" FROM "users" INNER JOIN LATERAL (SELECT "id", "user_id", "title", "created_at" FROM "posts" WHERE "user_id" = "users"."id" LIMIT 3) AS "recent" ON TRUE"""
    )
  }

  test("CROSS JOIN LATERAL — no .on required, transitions straight to SelectBuilder") {
    val af = users
      .crossJoinLateral(u => posts.select.where(p => p.user_id ==== u.id).limit(1).alias("top"))
      .select(r => (r.users.email, r.top.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "top"."title" FROM "users" CROSS JOIN LATERAL (SELECT "id", "user_id", "title", "created_at" FROM "posts" WHERE "user_id" = "users"."id" LIMIT 1) AS "top""""
    )
  }

  test("LEFT JOIN LATERAL — lateral cols decode as Option when the inner produces zero rows") {
    val q: QueryTemplate[?, (String, Option[String])] = users
      .leftJoinLateral(u => posts.select.where(p => p.user_id ==== u.id).limit(1).alias("top"))
      .on(_ => lit(true))
      .select(r => (r.users.email, r.top.title))
      .compile

    assert(q.fragment.sql.contains("LEFT JOIN LATERAL"), q.fragment.sql)
  }

  test("chained LATERAL after an INNER JOIN — multi-source outer view reaches inner WHERE") {
    val af = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .innerJoinLateral(r => tags.select.where(t => t.post_id ==== r.posts.id).limit(2).alias("top_tags"))
      .on(_ => lit(true))
      .select(r => (r.users.email, r.posts.title, r.top_tags.name))
      .compile
      .af

    assert(af.fragment.sql.contains("INNER JOIN LATERAL"), af.fragment.sql)
    assert(af.fragment.sql.contains("""WHERE "post_id" = "posts"."id""""), af.fragment.sql)
  }

  // ---- projected-SELECT `.alias` as subquery / lateral source ---------------------------------

  test("projected SELECT `.alias` renders as a derived table in FROM") {
    val af = users
      .select(u => (u.id, u.email))
      .where(u => u.age >= 18)
      .alias("adults")
      .select
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email" FROM (SELECT "id", "email" FROM "users" WHERE "age" >= $1) AS "adults""""
    )
  }

  test("projected SELECT `.alias` with `.as(\"lbl\")` expression — alias name surfaces as column name") {
    val af = users
      .select(u => (u.id, Pg.lower(u.email).as("lower_email")))
      .alias("u2")
      .select
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "lower_email" FROM (SELECT "id", lower("email") AS "lower_email" FROM "users") AS "u2""""
    )
  }

  test("projected SELECT joined with a base table — outer references derived cols by alias") {
    val af = users
      .select(u => (u.id, u.email))
      .where(u => u.age >= 18)
      .alias("adults")
      .innerJoin(posts).on(r => r.adults.id ==== r.posts.user_id)
      .select(r => (r.adults.email, r.posts.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "adults"."email", "posts"."title" FROM (SELECT "id", "email" FROM "users" WHERE "age" >= $1) AS "adults" INNER JOIN "posts" ON "adults"."id" = "posts"."user_id""""
    )
  }

  test("INNER JOIN LATERAL against a projected SELECT — per-outer-row top-N") {
    val af = users
      .innerJoinLateral(u =>
        posts.select(p => (p.id, p.title)).where(p => p.user_id ==== u.id).limit(3).alias("recent")
      )
      .on(_ => lit(true))
      .select(r => (r.users.email, r.recent.title))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "recent"."title" FROM "users" INNER JOIN LATERAL (SELECT "id", "title" FROM "posts" WHERE "user_id" = "users"."id" LIMIT 3) AS "recent" ON TRUE"""
    )
  }

  test("projected SELECT with an un-named expression fails to compile via AllNamedProj") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      case class U(id: UUID, email: String, age: Int)
      val t = Table.of[U]("users")
      t.select(u => (u.id, Pg.lower(u.email))).alias("x")
    """)
    assert(errs.nonEmpty, "expected compile error from AllNamedProj")
    assert(
      errs.exists(_.message.contains("requires every projected element")),
      errs.map(_.message).mkString("\n")
    )
  }

  test("LEFT then FULL — LEFT-nullabilified cols stay Option (no double-wrapping) under FULL") {
    // posts was already Option[_] from LEFT. FULL also nullabilifies users. The Scala type must NOT go to
    // Option[Option[_]] for posts — NullableCol is idempotent on already-nullable columns. In the inner ON of the
    // FULL JOIN, posts's cols are already Option (from LEFT) and tags's are still declared (non-null), so we use
    // raw comparisons there by round-tripping through literals that match both sides at the bare types.
    val q: QueryTemplate[?, (Option[String], Option[String], Option[String])] = users
      .leftJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .fullJoin(tags).on(_ => lit(true))
      .select(r => (r.users.email, r.posts.title, r.tags.name))
      .compile

    assert(q.fragment.sql.contains("FULL OUTER JOIN"), q.fragment.sql)
    assert(q.fragment.sql.contains("LEFT JOIN"), q.fragment.sql)
  }
}
