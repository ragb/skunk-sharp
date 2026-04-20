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

object ProjectionsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class ProjectionsSuite extends PgFixture {
  import ProjectionsSuite.User

  private val users = Table.of[User]("users")

  test("SELECT with single-column .project returns a list of values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        for {
          _ <- users.insert((
            id = id,
            email = "proj-single@example.com",
            age = 21,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.select.where(u => u.id === id).apply(u => u.email).compile.run(s),
            List("proj-single@example.com")
          )
        } yield ()
      }
    }
  }

  test("SELECT with .projectTuple returns a list of tuples") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        for {
          _ <- users.insert((
            id = id,
            email = "proj-tuple@example.com",
            age = 42,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.select.where(u => u.id === id).apply(u => (u.email, u.age)).compile.run(s),
            List(("proj-tuple@example.com", 42))
          )
        } yield ()
      }
    }
  }

  test("Function calls work in projections: lower(email)") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
        for {
          _ <- users.insert((
            id = id,
            email = "UpperCase@Example.com",
            age = 33,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.select.where(u => u.id === id).apply(u => Pg.lower(u.email)).compile.run(s),
            List("uppercase@example.com")
          )
        } yield ()
      }
    }
  }

}
