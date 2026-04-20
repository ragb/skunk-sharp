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

object SelectRelationSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
}

/**
 * Round-trip `.asRelation(alias)` against a real database. A filtered `users.select` is promoted to a derived relation,
 * then selected from and joined — verifying the outer `.compile` walks the stored thunk, that parameters flow through
 * correctly, and that the join sees the inner relation's column types.
 */
class SelectRelationSuite extends PgFixture {
  import SelectRelationSuite.*

  private val users = Table.of[User]("users").withDefault("created_at")
  private val posts = Table.of[Post]("posts").withDefault("created_at")

  test("select from a derived relation — only the filtered rows surface") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag     = "seniors"
        val youngId = UUID.randomUUID
        val oldId   = UUID.randomUUID
        val no      = Option.empty[OffsetDateTime]
        for {
          _ <- users.insert.values(
            (id = youngId, email = s"young-$tag@x", age = 20, deleted_at = no),
            (id = oldId, email = s"old-$tag@x", age = 70, deleted_at = no)
          ).compile.run(s)
          // Narrow by email pattern too so this assertion doesn't trip on rows seeded by sibling tests.
          seniors = users.select
            .where(u => u.age >= 60 && u.email.like(s"%-$tag@x"))
            .asRelation("seniors")
          _ <- assertIO(
            seniors.select(s0 => s0.email).compile.run(s).map(_.toSet),
            Set(s"old-$tag@x")
          )
        } yield ()
      }
    }
  }

  test("derived relation joins a base table — inner WHERE and outer ON both applied") {
    withContainers { containers =>
      session(containers).use { s =>
        val tag = "join"
        val u1  = UUID.randomUUID
        val u2  = UUID.randomUUID
        val no  = Option.empty[OffsetDateTime]
        for {
          _ <- users.insert.values(
            (id = u1, email = s"adult-$tag@x", age = 30, deleted_at = no),
            (id = u2, email = s"minor-$tag@x", age = 12, deleted_at = no)
          ).compile.run(s)
          _ <- posts.insert.values(
            (id = UUID.randomUUID, user_id = u1, title = s"adult-post-$tag"),
            (id = UUID.randomUUID, user_id = u2, title = s"minor-post-$tag")
          ).compile.run(s)
          adults = users.select
            .where(u => u.age >= 18 && u.email.like(s"%-$tag@x"))
            .asRelation("adults")
          _ <- assertIO(
            adults
              .innerJoin(posts)
              .on(r => r.adults.id ==== r.posts.user_id)
              .select(r => r.posts.title)
              .compile.run(s).map(_.toSet),
            Set(s"adult-post-$tag")
          )
        } yield ()
      }
    }
  }
}
