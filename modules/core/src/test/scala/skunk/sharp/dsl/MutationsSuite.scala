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
      .set(u => u.email := lit("new@example.com"))
      .where(u => u.id === Param.bind(UUID.fromString("00000000-0000-0000-0000-000000000001")))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = 'new@example.com' WHERE "id" = $1"""
    )
  }

  test("update with multiple SETs (tuple form)") {
    val af = users.update
      .set(u => (u.email := lit("x"), u.age := lit(42)))
      .where(u => u.id === Param.bind(UUID.fromString("00000000-0000-0000-0000-000000000001")))
      .compile.af

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = 'x', "age" = 42 WHERE "id" = $1"""
    )
  }

  test("delete with WHERE") {
    val af = users.delete.where(u => u.email === lit("gone@example.com")).compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "email" = 'gone@example.com'""")
  }

  test(".deleteAll explicitly opts into an unconditional DELETE") {
    val af = users.delete.deleteAll.compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users"""")
  }

  test(".updateAll explicitly opts into an unconditional UPDATE") {
    val af = users.update.set(u => u.age := lit(0)).updateAll.compile.af
    assertEquals(af.fragment.sql, """UPDATE "users" SET "age" = 0""")
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
      users.update.set(u => u.age := lit(0)).compile
    """)
    assert(err.nonEmpty, "expected compile error: .compile only exists after .where or .updateAll")
  }

  test("update .returning appends RETURNING <col>") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.email := lit("x"))
      .where(u => u.id === Param.bind(id))
      .returning(u => u.id)
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = 'x' WHERE "id" = $1 RETURNING "id""""
    )
  }

  test("update .returningTuple returns multiple columns") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .set(u => u.age := lit(42))
      .where(u => u.id === Param.bind(id))
      .returningTuple(u => (u.id, u.age))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = 42 WHERE "id" = $1 RETURNING "id", "age""""
    )
  }

  test("update .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update.set(u => u.age := lit(42)).where(u => u.id === Param.bind(id)).returningAll.compile.af
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = 42 WHERE "id" = $1 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }

  test("delete .returning") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === Param.bind(id)).returning(u => u.email).compile.af
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "id" = $1 RETURNING "email"""")
  }

  test("delete .returningAll returns the whole row") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.delete.where(u => u.id === Param.bind(id)).returningAll.compile.af
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
      .where(u => u.id === Param.bind(id))
      .compile.af

    assertEquals(af.fragment.sql, """UPDATE "users" SET "email" = $1 WHERE "id" = $2""")
  }

  test(".patch — multiple Somes render in the order the named tuple lists them") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val af = users.update
      .patch((email = Some("x"), age = Some(42)))
      .where(u => u.id === Param.bind(id))
      .compile.af

    assertEquals(af.fragment.sql, """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3""")
  }

  test(".patch — nullable column: Some(None) sets to NULL, Some(Some(v)) sets to v") {
    val ts      = OffsetDateTime.parse("2024-01-01T00:00:00Z")
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val afClear = users.update
      .patch((deleted_at = Some(Option.empty[OffsetDateTime])))
      .where(u => u.id === Param.bind(id))
      .compile.af
    assertEquals(afClear.fragment.sql, """UPDATE "users" SET "deleted_at" = $1 WHERE "id" = $2""")

    val afSet = users.update
      .patch((deleted_at = Some(Some(ts))))
      .where(u => u.id === Param.bind(id))
      .compile.af
    assertEquals(afSet.fragment.sql, """UPDATE "users" SET "deleted_at" = $1 WHERE "id" = $2""")
  }

  test(".patch with all-None throws at runtime — empty SET list is a user error") {
    val id = UUID.fromString("00000000-0000-0000-0000-000000000001")
    intercept[IllegalArgumentException] {
      users.update
        .patch((email = Option.empty[String], age = Option.empty[Int]))
        .where(u => u.id === Param.bind(id))
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
      .where(u => u.id === Param.bind(id))
      .returning(u => u.email)
      .compile.af
    assert(af.fragment.sql.endsWith(""" RETURNING "email""""), af.fragment.sql)
  }

  // ---- Tuple-form SET carries typed Args ----

  test("tuple SET folds each assignment's Args: Params reach .compile, baked values stay out") {
    val q: CommandTemplate[(String, Int)] = users.update
      .set(u => (u.email := Param[String], u.age := Param[Int], u.deleted_at := Pg.nullOf[OffsetDateTime]))
      .where(u => u.id === Param.bind(UUID.fromString("00000000-0000-0000-0000-000000000001")))
      .compile
    assertEquals(q.fragment.sql, """UPDATE "users" SET "email" = $1, "age" = $2, "deleted_at" = NULL WHERE "id" = $3""")
    // Previously this typed as Void and threw ClassCastException here.
    assertEquals(
      q.fragment.encoder.encode(("x@y", 30)).flatten.map(_.value),
      List("x@y", "30", "00000000-0000-0000-0000-000000000001")
    )
  }

  test("all-baked tuple SET stays Void") {
    val q: CommandTemplate[skunk.Void] =
      users.update.set(u => (u.email := "a", u.age := 1)).updateAll.compile
    assertEquals(q.fragment.sql, """UPDATE "users" SET "email" = 'a', "age" = 1""")
  }

  test("tuple ON CONFLICT DO UPDATE and doUpdateFromExcluded thread Params too") {
    val users2 = Table.of[User]("users").withPrimary("id")
    val row    = (
      id = UUID.fromString("00000000-0000-0000-0000-000000000002"),
      email = "e",
      age = 1,
      created_at = OffsetDateTime.parse("2026-01-01T00:00:00Z"),
      deleted_at = Option.empty[OffsetDateTime]
    )
    val a = users2.insert(row).onConflict(u => u.id).doUpdate(u => (u.email := Param[String], u.age := 2)).compile
    val b = users2.insert(row).onConflict(u => u.id)
      .doUpdateFromExcluded((u, ex) => (u.email := ex.email, u.age := Param[Int])).compile
    val _: CommandTemplate[String] = a
    val _: CommandTemplate[Int]    = b
    assert(a.fragment.sql.endsWith("""ON CONFLICT ("id") DO UPDATE SET "email" = $6, "age" = 2"""), a.fragment.sql)
    assert(
      b.fragment.sql.endsWith("""ON CONFLICT ("id") DO UPDATE SET "email" = excluded."email", "age" = $6"""),
      b.fragment.sql
    )
  }
}
