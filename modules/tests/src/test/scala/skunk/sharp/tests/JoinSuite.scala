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

package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object JoinSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
  case class Tag(id: UUID, post_id: UUID, name: String)
}

class JoinSuite extends PgFixture {
  import JoinSuite.*

  private val users = Table.of[User]("users").withDefault("created_at")
  private val posts = Table.of[Post]("posts").withDefault("created_at")
  private val tags  = Table.of[Tag]("tags")

  test("INNER JOIN round-trips users + posts (auto-alias)") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "join-u@x", age = 30, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = pid, user_id = uid, title = "hello")).compile.run(s)
          _ <- assertIO(
            users
              .innerJoin(posts)
              .on(r => r.users.id ==== r.posts.user_id)
              .select(r => (r.users.email, r.posts.title))
              .where(r => r.posts.id === pid)
              .compile
              .run(s),
            List(("join-u@x", "hello"))
          )
        } yield ()
      }
    }
  }

  test("LEFT JOIN returns NULL on the right side when no match (auto-alias)") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "solo@x", age = 40, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- assertIO(
            users
              .leftJoin(posts)
              .on(r => r.users.id ==== r.posts.user_id)
              .select(r => (r.users.email, r.posts.title))
              .where(r => r.users.id === uid)
              .compile
              .run(s),
            List(("solo@x", Option.empty[String]))
          )
        } yield ()
      }
    }
  }

  test("JOIN + GROUP BY + COUNT: posts per user (auto-alias)") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "many@x", age = 28, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert.values(
            (id = UUID.randomUUID, user_id = uid, title = "a"),
            (id = UUID.randomUUID, user_id = uid, title = "b"),
            (id = UUID.randomUUID, user_id = uid, title = "c")
          ).compile.run(s)
          _ <- assertIO(
            users
              .leftJoin(posts)
              .on(r => r.users.id ==== r.posts.user_id)
              .select(r => (r.users.email, Pg.count(r.posts.id).as("n")))
              .where(r => r.users.id === uid)
              .groupBy(r => r.users.email)
              .compile
              .run(s),
            List(("many@x", 3L))
          )
        } yield ()
      }
    }
  }

  test("three-table chain INNER + LEFT with auto-alias") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "chain@x", age = 33, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = pid, user_id = uid, title = "chain-post")).compile.run(s)
          _ <- tags.insert.values(
            (id = UUID.randomUUID, post_id = pid, name = "alpha"),
            (id = UUID.randomUUID, post_id = pid, name = "beta")
          ).compile.run(s)
          _ <- assertIO(
            users
              .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
              .leftJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
              .select(r => (r.users.email, r.posts.title, r.tags.name))
              .where(r => r.users.id === uid)
              .orderBy(r => r.tags.name.asc)
              .compile
              .run(s),
            List(
              ("chain@x", "chain-post", Option("alpha")),
              ("chain@x", "chain-post", Option("beta"))
            )
          )
        } yield ()
      }
    }
  }

  test(".crossJoin renders CROSS JOIN; WHERE supplies the join predicate") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "cross@x", age = 27, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = pid, user_id = uid, title = "cross-post")).compile.run(s)
          _ <- assertIO(
            users
              .crossJoin(posts)
              .select(r => (r.users.email, r.posts.title))
              .where(r => r.users.id ==== r.posts.user_id && r.users.id === uid)
              .compile
              .run(s),
            List(("cross@x", "cross-post"))
          )
        } yield ()
      }
    }
  }

  test("explicit .alias(...) still works — end-to-end sanity") {
    withContainers { containers =>
      session(containers).use { s =>
        val uid = UUID.randomUUID
        val pid = UUID.randomUUID
        for {
          _ <- users
            .insert((id = uid, email = "aliased@x", age = 22, deleted_at = Option.empty[OffsetDateTime]))
            .compile.run(s)
          _ <- posts.insert((id = pid, user_id = uid, title = "aliased-hello")).compile.run(s)
          _ <- assertIO(
            users
              .alias("u")
              .innerJoin(posts.alias("p"))
              .on(r => r.u.id ==== r.p.user_id)
              .select(r => (r.u.email, r.p.title))
              .where(r => r.p.id === pid)
              .compile
              .run(s),
            List(("aliased@x", "aliased-hello"))
          )
        } yield ()
      }
    }
  }
}
