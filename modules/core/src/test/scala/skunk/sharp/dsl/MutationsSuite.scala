package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object MutationsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class MutationsSuite extends munit.FunSuite {
  import MutationsSuite.User

  private val users = Table.of[User]("users")

  test("update with single SET and WHERE") {
    val af = users.update
      .set(u => u.email := "new@example.com")
      .where(u => u.id === UUID.fromString("00000000-0000-0000-0000-000000000001"))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2"""
    )
  }

  test("update with multiple SETs (tuple form)") {
    val af = users.update
      .set(u => (u.email := "x", u.age := 42))
      .where(u => u.id === UUID.fromString("00000000-0000-0000-0000-000000000001"))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3"""
    )
  }

  test("delete with WHERE") {
    val af = users.delete.where(u => u.email === "gone@example.com").compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "email" = $1""")
  }

  test(".deleteAll explicitly opts into an unconditional DELETE") {
    val af = users.delete.deleteAll.compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users"""")
  }

  test(".updateAll explicitly opts into an unconditional UPDATE") {
    val af = users.update.set(u => u.age := 0).updateAll.compile.af
    assertEquals(af.fragment.sql, """UPDATE "users" SET "age" = $1""")
  }

  test("delete without .where or .deleteAll does not compile") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.delete.compile
    """)
    assert(err.nonEmpty, "expected compile error: .compile only exists after .where or .deleteAll")
  }

  test("update without .where or .updateAll does not compile") {
    val err = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.update.set(u => u.age := 0).compile
    """)
    assert(err.nonEmpty, "expected compile error: .compile only exists after .where or .updateAll")
  }

  test("update .returning appends RETURNING <col>") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.email := "x")
      .where(u => u.id === id)
      .returning(u => u.id)
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2 RETURNING "id""""
    )
  }

  test("update .returningTuple returns multiple columns") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.age := 42)
      .where(u => u.id === id)
      .returningTuple(u => (u.id, u.age))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "age""""
    )
  }

  test("update .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update.set(u => u.age := 42).where(u => u.id === id).returningAll.compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }

  test("delete .returning") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === id).returning(u => u.email).compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "id" = $1 RETURNING "email"""")
  }

  test("delete .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === id).returningAll.compile.af
    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" WHERE "id" = $1 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }

  // ---- .patch ---------------------------------------------------------------------------------

  test(".patch — only the Some fields reach the SET list, Nones are dropped") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .patch((email = Some("new@x"), age = None, deleted_at = None))
      .where(u => u.id === id)
      .compile.af

    assertEquals(af.fragment.sql, """UPDATE "users" SET "email" = $1 WHERE "id" = $2""")
  }

  test(".patch — multiple Somes render in the order the named tuple lists them") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .patch((email = Some("x"), age = Some(42)))
      .where(u => u.id === id)
      .compile.af

    assertEquals(af.fragment.sql, """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3""")
  }

  test(".patch — nullable column: Some(None) sets to NULL, Some(Some(v)) sets to v") {
    val ts      = OffsetDateTime.parse("2024-01-01T00:00:00Z")
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val afClear = users.update
      .patch((deleted_at = Some(Option.empty[OffsetDateTime])))
      .where(u => u.id === id)
      .compile.af
    assertEquals(afClear.fragment.sql, """UPDATE "users" SET "deleted_at" = $1 WHERE "id" = $2""")

    val afSet = users.update
      .patch((deleted_at = Some(Some(ts))))
      .where(u => u.id === id)
      .compile.af
    assertEquals(afSet.fragment.sql, """UPDATE "users" SET "deleted_at" = $1 WHERE "id" = $2""")
  }

  test(".patch with all-None throws at runtime — empty SET list is a user error") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    intercept[IllegalArgumentException] {
      users.update
        .patch((email = Option.empty[String], age = Option.empty[Int]))
        .where(u => u.id === id)
        .compile
    }
  }

  test(".patch rejects unknown field names at compile time") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.update.patch((unknown_field = Some(1))).updateAll
    """)
    assert(errs.nonEmpty, "expected compile error for unknown field")
  }

  test(".patch rejects wrong value type at compile time") {
    // age: Int → patch value must be Option[Int]. Passing Option[String] is a compile error.
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[MutationsSuite.User]("users")
      users.update.patch((age = Some("not-an-int"))).updateAll
    """)
    assert(errs.nonEmpty, "expected compile error for wrong value type")
  }

  test(".patch on a nullable column rejects a single-wrapped Some — you need Some(Some(v))") {
    // deleted_at: Option[OffsetDateTime] → patch value must be Option[Option[OffsetDateTime]]. A bare
    // Some(ts: OffsetDateTime) would be Some[OffsetDateTime] which isn't a subtype of Option[Option[OffsetDateTime]].
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.time.OffsetDateTime
      val users = Table.of[MutationsSuite.User]("users")
      val ts    = OffsetDateTime.parse("2024-01-01T00:00:00Z")
      users.update.patch((deleted_at = Some(ts))).updateAll
    """)
    assert(errs.nonEmpty, "expected compile error for single-wrapped Some on a nullable column")
  }

  test(".patch chains into .returning*") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .patch((email = Some("new@x")))
      .where(u => u.id === id)
      .returning(u => u.email)
      .compile.af
    assert(af.fragment.sql.endsWith(""" RETURNING "email""""), af.fragment.sql)
  }
}
