package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.util.UUID

object SetOpSuite {
  case class User(id: UUID, email: String, age: Int)
}

class SetOpSuite extends munit.FunSuite {
  import SetOpSuite.*

  private val users  = Table.of[User]("users")
  private val admins = Table.of[User]("admins")

  // ---- Basic set ops directly on SelectBuilder (uncompiled) -------------------------------------

  test("UNION of two whole-row selects — single .compile, both arms rendered with parens") {
    val af = users.select.union(admins.select).compile.af

    assertEquals(
      af.fragment.sql,
      """(SELECT "id", "email", "age" FROM "users") UNION (SELECT "id", "email", "age" FROM "admins")"""
    )
  }

  test("UNION ALL keeps duplicates") {
    val af = users.select.unionAll(admins.select).compile.af

    assertEquals(
      af.fragment.sql,
      """(SELECT "id", "email", "age" FROM "users") UNION ALL (SELECT "id", "email", "age" FROM "admins")"""
    )
  }

  test("INTERSECT / INTERSECT ALL / EXCEPT / EXCEPT ALL render the matching keyword") {
    def sqlOf(q: SetOpQuery[?]): String = q.compile.fragment.sql

    assert(sqlOf(users.select.intersect(admins.select)).contains(" INTERSECT ("))
    assert(sqlOf(users.select.intersectAll(admins.select)).contains(" INTERSECT ALL ("))
    assert(sqlOf(users.select.except(admins.select)).contains(" EXCEPT ("))
    assert(sqlOf(users.select.exceptAll(admins.select)).contains(" EXCEPT ALL ("))
  }

  // ---- Chaining keeps a single .compile at the very end -----------------------------------------

  test("chained UNION then INTERSECT — left-to-right, each arm parenthesised") {
    val af = users.select.union(admins.select).intersect(users.select).compile.af

    assertEquals(
      af.fragment.sql,
      """(SELECT "id", "email", "age" FROM "users") UNION (SELECT "id", "email", "age" FROM "admins") INTERSECT (SELECT "id", "email", "age" FROM "users")"""
    )
  }

  test("chained set ops preserve parameters from every arm") {
    val af = users.select
      .where(u => u.age >= 18)
      .union(admins.select.where(a => a.age >= 21))
      .except(users.select.where(u => u.age >= 65))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """(SELECT "id", "email", "age" FROM "users" WHERE "age" >= $1) UNION (SELECT "id", "email", "age" FROM "admins" WHERE "age" >= $2) EXCEPT (SELECT "id", "email", "age" FROM "users" WHERE "age" >= $3)"""
    )
  }

  // ---- Mixing builder / projected / already-compiled arms ---------------------------------------

  test("UNION of projected selects — same projection shape on both sides") {
    val af = users
      .select(u => (u.email, u.age))
      .union(admins.select(a => (a.email, a.age)))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """(SELECT "email", "age" FROM "users") UNION (SELECT "email", "age" FROM "admins")"""
    )
  }

  test("UNION accepts a right-hand SetOpQuery — enables nested set-op composition") {
    val inner = users.select.union(admins.select)
    val af    = users.select.union(inner).compile.af

    assertEquals(
      af.fragment.sql,
      """(SELECT "id", "email", "age" FROM "users") UNION ((SELECT "id", "email", "age" FROM "users") UNION (SELECT "id", "email", "age" FROM "admins"))"""
    )
  }

  // ---- SetOpQuery used as a subquery via .asExpr ------------------------------------------------

  test("SetOpQuery slots into .in(...) — row-compatible subquery over UNION") {
    val activeIds = users.select(u => u.id).union(admins.select(a => a.id))
    val af        = users.select.where(u => u.id.in(activeIds)).compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "email", "age" FROM "users" WHERE "id" IN ((SELECT "id" FROM "users") UNION (SELECT "id" FROM "admins"))"""
    )
  }
}
