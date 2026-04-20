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

object InsertReturningSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class InsertReturningSuite extends PgFixture {
  import InsertReturningSuite.User

  private val users = Table.of[User]("users").withDefault("created_at")

  test("INSERT ... RETURNING single column yields the inserted value") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd")
        for {
          _ <- assertIO(
            users
              .insert((
                id = id,
                email = "ret-single@example.com",
                age = 27,
                created_at = OffsetDateTime.now(),
                deleted_at = None
              ))
              .returning(u => u.id)
              .compile.unique(s),
            id
          )
        } yield ()
      }
    }
  }

  test("INSERT ... RETURNING tuple yields the inserted values as a tuple") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
        for {
          _ <- assertIO(
            users
              .insert((
                id = id,
                email = "ret-tuple@example.com",
                age = 55,
                created_at = OffsetDateTime.now(),
                deleted_at = None
              ))
              .returningTuple(u => (u.id, u.age))
              .compile.unique(s),
            (id, 55)
          )
        } yield ()
      }
    }
  }
}
