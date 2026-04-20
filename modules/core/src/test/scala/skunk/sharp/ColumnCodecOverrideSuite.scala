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

import skunk.codec.all.*
import skunk.data.Type
import skunk.sharp.dsl.*

import java.util.UUID

object ColumnCodecOverrideSuite {
  case class User(id: UUID, email: String, age: Int)
}

class ColumnCodecOverrideSuite extends munit.FunSuite {
  import ColumnCodecOverrideSuite.User

  test("Table.of picks default PgTypeFor codec (String → text, Int → int4, UUID → uuid)") {
    val users = Table.of[User]("users")
    val cols  = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.text, Type.int4))
  }

  test("withColumnCodec overrides codec — tpe is read from the new codec") {
    val users = Table.of[User]("users").withColumnCodec("email", varchar(256))
    val cols  = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.varchar(256), Type.int4))
  }

  test("withColumn is the primitive; sugar methods delegate to it") {
    val users = Table.of[User]("users").withColumn("email")(c =>
      c.copy(attrs = List(ColumnAttrValue.Pk(List("email")), ColumnAttrValue.Uq("email", List("email"))))
    )
    val email = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].find(_.name == "email").get
    assertEquals(email.isPrimary, true)
    assertEquals(email.isUnique, true)
  }

  test("builder .column takes a codec directly — tpe is codec.types.head") {
    val users = Table
      .builder("users")
      .column("id", uuid)
      .column("email", varchar(320))
      .column("age", int8)
      .build
    val cols = users.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.varchar(320), Type.int8))
  }

  test("TypedExpr.cast appends a Postgres ::type cast using the codec's short type name") {
    val users = Table.of[User]("users")
    val af    = users.select(u => u.age.cast[Long]).compile.af
    assertEquals(af.fragment.sql, """SELECT "age"::int8 FROM "users"""")
  }

  test("cast can be used in WHERE comparisons") {
    val users = Table.of[User]("users")
    val af    = users.select.where(u => u.id.cast[String] === "abc").compile.af
    assertEquals(af.fragment.sql, """SELECT "id", "email", "age" FROM "users" WHERE "id"::text = $1""")
  }
}
