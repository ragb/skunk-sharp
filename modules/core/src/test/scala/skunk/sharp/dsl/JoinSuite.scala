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
}
