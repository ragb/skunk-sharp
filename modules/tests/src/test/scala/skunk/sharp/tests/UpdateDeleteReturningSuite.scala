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

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object UpdateDeleteReturningSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class UpdateDeleteReturningSuite extends PgFixture {
  import UpdateDeleteReturningSuite.User

  private val users = Table.of[User]("users").withDefault("created_at")

  test("UPDATE ... RETURNING returns the new value") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("77777777-7777-7777-7777-777777777777")
        for {
          _ <- users.insert((
            id = id,
            email = "old@example.com",
            age = 20,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.update
              .set(u => u.email := "new@example.com")
              .where(u => u.id === id)
              .returning(u => u.email)
              .compile.unique(s),
            "new@example.com"
          )
        } yield ()
      }
    }
  }

  test("UPDATE ... RETURNING returningTuple returns multiple values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("88888888-8888-8888-8888-888888888888")
        for {
          _ <- users.insert((
            id = id,
            email = "tuple-ret@example.com",
            age = 10,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.update
              .set(u => u.age := 99)
              .where(u => u.id === id)
              .returningTuple(u => (u.id, u.age))
              .compile.unique(s),
            (id, 99)
          )
        } yield ()
      }
    }
  }

  test("DELETE ... RETURNING yields the deleted row values") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.fromString("99999999-9999-9999-9999-999999999999")
        for {
          _ <- users.insert((
            id = id,
            email = "gone@example.com",
            age = 1,
            created_at = OffsetDateTime.now(),
            deleted_at = None
          )).compile.run(s)
          _ <- assertIO(
            users.delete.where(u => u.id === id).returning(u => u.email).compile.unique(s),
            "gone@example.com"
          )
        } yield ()
      }
    }
  }
}
