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
