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
      .compile

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2"""
    )
  }

  test("update with multiple SETs (tuple form)") {
    val af = users.update
      .set(u => (u.email := "x", u.age := 42))
      .where(u => u.id === UUID.fromString("00000000-0000-0000-0000-000000000001"))
      .compile

    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3"""
    )
  }

  test("delete with WHERE") {
    val af = users.delete.where(u => u.email === "gone@example.com").compile
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "email" = $1""")
  }

  test("delete without WHERE compiles to unconditional DELETE") {
    val af = users.delete.compile
    assertEquals(af.fragment.sql, """DELETE FROM "users"""")
  }

  test("update .returning appends RETURNING <col>") {
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val (af, _) = users.update
      .set(u => u.email := "x")
      .where(u => u.id === id)
      .returning(u => u.id)
      .compile
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "email" = $1 WHERE "id" = $2 RETURNING "id""""
    )
  }

  test("update .returningTuple returns multiple columns") {
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val (af, _) = users.update
      .set(u => u.age := 42)
      .where(u => u.id === id)
      .returningTuple(u => (u.id, u.age))
      .compile
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "age""""
    )
  }

  test("update .returningAll returns the whole row") {
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val (af, _) = users.update.set(u => u.age := 42).where(u => u.id === id).returningAll.compile
    assertEquals(
      af.fragment.sql,
      """UPDATE "users" SET "age" = $1 WHERE "id" = $2 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }

  test("delete .returning") {
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val (af, _) = users.delete.where(u => u.id === id).returning(u => u.email).compile
    assertEquals(af.fragment.sql, """DELETE FROM "users" WHERE "id" = $1 RETURNING "email"""")
  }

  test("delete .returningAll returns the whole row") {
    val id      = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val (af, _) = users.delete.where(u => u.id === id).returningAll.compile
    assertEquals(
      af.fragment.sql,
      """DELETE FROM "users" WHERE "id" = $1 RETURNING "id", "email", "age", "created_at", "deleted_at""""
    )
  }
}
