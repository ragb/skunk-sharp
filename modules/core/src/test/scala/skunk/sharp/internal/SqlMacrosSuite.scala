package skunk.sharp.internal

import skunk.sharp.dsl.*

import java.util.UUID

object SqlMacrosSuite {
  case class User(id: UUID, email: String, age: Int)
}

class SqlMacrosSuite extends munit.FunSuite {
  import SqlMacrosSuite.*

  private val users = Table.of[User]("users")

  test("columnValOp baked fragment matches runtime ===") {
    // Runtime path via existing === operator.
    val runtime = users.select.where(u => u.age === 18).compile.af

    // Macro-baked path: reach into the same WHERE construction via the new inline helper,
    // feeding it the TypedColumn directly.
    val baked = {
      // Access the same TypedColumn that the DSL uses inside a WHERE lambda.
      // The simplest way: use the same `select.where` shape but route through the macro.
      users.select.where(u => SqlMacros.columnValOp(u.age, "=", 18)).compile.af
    }

    assertEquals(runtime.fragment.sql, baked.fragment.sql)
  }

  test("columnValOp on String renders quoted identifier + encoder placeholder") {
    val baked = users.select.where(u => SqlMacros.columnValOp(u.email, "=", "a@b.com")).compile.af
    // Should be: SELECT ... FROM "users" WHERE "email" = $1
    assertEquals(baked.fragment.sql, """SELECT "id", "email", "age" FROM "users" WHERE "email" = $1""")
  }

  test("columnValOp with non-equality operator (>=) works") {
    val baked = users.select.where(u => SqlMacros.columnValOp(u.age, ">=", 21)).compile.af
    assertEquals(baked.fragment.sql, """SELECT "id", "email", "age" FROM "users" WHERE "age" >= $1""")
  }

}
