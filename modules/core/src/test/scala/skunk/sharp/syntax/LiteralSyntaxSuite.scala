package skunk.sharp.syntax

import scala.compiletime.testing.typeCheckErrors
import skunk.sharp.{ColumnsView, Table}
import skunk.sharp.dsl.*

import java.util.UUID

object LiteralSyntaxSuite {
  case class User(id: UUID, email: String, age: Int, score: Double, active: Boolean)
}

/**
 * Covers the singleton-typed `given Conversion`s shipped from `TypedExpr`'s companion object. Because they live there —
 * and because `TypedExpr` is declared `into trait` — primitive literals slot directly into any operator's `TypedExpr`
 * RHS slot without needing an import or `language.implicitConversions`. The conversion fires only for singleton-typed
 * sources (true literals and stable `val`s); method calls, vars and widened expressions are rejected.
 */
class LiteralSyntaxSuite extends munit.FunSuite {
  import LiteralSyntaxSuite.User

  private val users = Table.of[User]("users")
  private val cols  = ColumnsView(users.columns)

  // -------- Literal RHS bakes inline -----------------------------------------------------------

  test("Int literal → inline `\"col\" = 18`") {
    val w = cols.age === 18
    assertEquals(w.fragment.sql, """"age" = 18""")
    val _: skunk.sharp.where.Where[skunk.Void] = w
  }

  test("String literal → quoted, single-quote-escaped inline SQL") {
    val w = cols.email === "alice@example.com"
    assertEquals(w.fragment.sql, """"email" = 'alice@example.com'""")
  }

  test("String literal with embedded single quote escapes correctly") {
    val w = cols.email === "O'Brien"
    assertEquals(w.fragment.sql, """"email" = 'O''Brien'""")
  }

  test("Boolean literal renders TRUE / FALSE inline") {
    assertEquals((cols.active === true).fragment.sql, """"active" = TRUE""")
    assertEquals((cols.active === false).fragment.sql, """"active" = FALSE""")
  }

  test("Double literal renders with the ::float8 cast") {
    val w = cols.score >= 0.5d
    assertEquals(w.fragment.sql, """"score" >= 0.5::float8""")
  }

  test("Every comparison operator accepts a literal RHS and bakes it inline") {
    assertEquals((cols.age !== 0).fragment.sql, """"age" <> 0""")
    assertEquals((cols.age < 100).fragment.sql, """"age" < 100""")
    assertEquals((cols.age <= 100).fragment.sql, """"age" <= 100""")
    assertEquals((cols.age > 0).fragment.sql, """"age" > 0""")
    assertEquals((cols.age >= 18).fragment.sql, """"age" >= 18""")
  }

  // -------- Negative: non-singleton sources are rejected ---------------------------------------

  test("a method-call result is rejected (no Singleton)") {
    val errors = typeCheckErrors(
      """
        import skunk.sharp.dsl.*
        import skunk.sharp.{ColumnsView, Table}
        import java.util.UUID
        case class U(id: UUID, age: Int)
        val t = Table.of[U]("u")
        val cv = ColumnsView(t.columns)
        def randAge(): Int = scala.util.Random.nextInt(100)
        cv.age === randAge()
      """
    )
    assert(errors.nonEmpty, "expected method-call RHS to fail (return type Int has no Singleton)")
  }

  test("a `var` reference is rejected (vars don't have stable singleton types)") {
    val errors = typeCheckErrors(
      """
        import skunk.sharp.dsl.*
        import skunk.sharp.{ColumnsView, Table}
        import java.util.UUID
        case class U(id: UUID, age: Int)
        val t = Table.of[U]("u")
        val cv = ColumnsView(t.columns)
        var v = 42
        cv.age === v
      """
    )
    assert(errors.nonEmpty, "expected `var v` to fail")
  }

  test("an explicitly widened-Int reference is rejected") {
    // `(v: Int)` ascription erases the val's singleton path-dependent type, leaving plain Int.
    val errors = typeCheckErrors(
      """
        import skunk.sharp.dsl.*
        import skunk.sharp.{ColumnsView, Table}
        import java.util.UUID
        case class U(id: UUID, age: Int)
        val t = Table.of[U]("u")
        val cv = ColumnsView(t.columns)
        val raw = 42
        cv.age === (raw: Int)
      """
    )
    assert(errors.nonEmpty, "expected widened (raw: Int) to fail (no Singleton)")
  }

  // -------- Existing TypedExpr-RHS paths preserved ---------------------------------------------

  test("col === Param[T] still works (deferred parameter)") {
    val w = cols.age === Param[Int]
    assertEquals(w.fragment.sql, """"age" = $1""")
    val _: skunk.sharp.where.Where[Int] = w
  }

  test("col === lit(v) still works (explicit-lit form, same SQL as the new shorthand)") {
    val w = cols.age === 18
    assertEquals(w.fragment.sql, """"age" = 18""")
  }

  test("col1 === col2 still works (column-vs-column)") {
    val w = cols.age === cols.age
    assertEquals(w.fragment.sql, """"age" = "age"""")
  }

  test("col === Param.bind(v) still works") {
    val w = cols.age === Param.bind(42)
    assertEquals(w.fragment.sql, """"age" = $1""")
  }

  // -------- Composition --------------------------------------------------------------------------

  test("literal-RHS Wheres compose with && producing Where[Void]") {
    val w = cols.age >= 18 && cols.age <= 65 && cols.email === "alice@example.com"
    assertEquals(
      w.fragment.sql,
      """(("age" >= 18 AND "age" <= 65) AND "email" = 'alice@example.com')"""
    )
    val _: skunk.sharp.where.Where[skunk.Void] = w
  }

  test("literal-RHS Wheres compose with allOf(...)") {
    val w = allOf(cols.age >= 0, cols.age <= 100, cols.active === true)
    assertEquals(w.fragment.sql, """(("age" >= 0 AND "age" <= 100) AND "active" = TRUE)""")
  }
}
