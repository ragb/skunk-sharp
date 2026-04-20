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

package skunk.sharp

import skunk.data.Type

import java.time.OffsetDateTime
import java.util.UUID

object TableOfSuite {
  case class User(id: UUID, email: String, createdAt: OffsetDateTime, deletedAt: Option[OffsetDateTime])

  /**
   * Unbounded opaque alias — the shape that refined 0.11.x uses for `Refined[T, P]`. No `<: T` upper bound visible
   * outside this object, so match types can't prove disjointness from `Option[_]`. The typeclass-based
   * [[skunk.sharp.internal.DeriveColumns]] tolerates this; locking in #54.
   */
  opaque type Opaque[+T] = T

  object Opaque {
    def apply[T](t: T): Opaque[T] = t

    // Fallback PgTypeFor — mirrors refined's `refinedPgTypeFor` / iron's `refinedPgTypeFor` pattern.
    given opaquePgTypeFor[T](using base: skunk.sharp.pg.PgTypeFor[T]): skunk.sharp.pg.PgTypeFor[Opaque[T]] =
      skunk.sharp.pg.PgTypeFor.instance(base.codec.asInstanceOf[skunk.Codec[Opaque[T]]])

  }

  case class Person(id: Int, email: Opaque[String], age: Opaque[Int])
}

class TableOfSuite extends munit.FunSuite {
  import TableOfSuite.User

  test("Table.of derives columns from a case class Mirror; tpe is the skunk Type from the codec") {
    val users = Table.of[User]("users")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "createdAt", "deletedAt"))
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.text, Type.timestamptz, Type.timestamptz))
    assertEquals(cols.map(_.isNullable), List(false, false, false, true))
    assertEquals(cols.map(_.hasDefault), List(false, false, false, false))
  }

  test("withPrimary / withUnique update runtime metadata") {
    val users = Table.of[User]("users").withPrimary("id").withUnique("email")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.name).zip(cols.map(_.isPrimary)),
      List("id" -> true, "email" -> false, "createdAt" -> false, "deletedAt" -> false)
    )
    assertEquals(
      cols.map(_.name).zip(cols.map(_.isUnique)),
      List("id" -> false, "email" -> true, "createdAt" -> false, "deletedAt" -> false)
    )
  }

  test("Table.of handles unbounded opaque aliases (regression for #54)") {
    // Without typeclass dispatch, `Person.email: Opaque[String]` would stall ColumnsFromMirror's Option[_] check
    // because an unbounded opaque isn't provably disjoint from `Option`. The typeclass-based DeriveColumns resolves
    // cleanly via `NotGiven[T <:< Option[?]]`.
    val people = Table.of[TableOfSuite.Person]("people")

    val cols = people.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "age"))
    assertEquals(cols.map(_.isNullable), List(false, false, false))
  }

  test("withDefault flips runtime hasDefault flag and advances the Default phantom") {
    val users = Table.of[User]("users").withDefault("createdAt")

    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.name).zip(cols.map(_.hasDefault)),
      List("id" -> false, "email" -> false, "createdAt" -> true, "deletedAt" -> false)
    )

    summon[ColumnHasDefault[users.columns.type, "createdAt"] =:= true]
    summon[ColumnHasDefault[users.columns.type, "id"] =:= false]
  }
}

/** Local helper: checks whether the column named `N` in `Cols` has the `ColumnAttr.Default` marker. */
type ColumnHasDefault[Cols <: Tuple, N <: String & Singleton] = skunk.sharp.ColumnDefault[Cols, N]
