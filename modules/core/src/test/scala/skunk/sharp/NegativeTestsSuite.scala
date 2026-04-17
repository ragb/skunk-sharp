package skunk.sharp

import scala.compiletime.testing.*

import java.time.OffsetDateTime
import java.util.UUID

object NegativeTestsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class NegativeTestsSuite extends munit.FunSuite {

  test("withPrimary rejects unknown column names with a friendly message listing columns") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import NegativeTestsSuite.User
      Table.of[User]("users").withPrimary("nope")
    """)
    assert(errs.nonEmpty, "expected a compile error")
    val msg = errs.map(_.message).mkString("\n")
    assert(msg.contains("\"nope\""), s"error should name the bad column; got: $msg")
    assert(msg.contains("id"), s"error should list available columns; got: $msg")
    assert(msg.contains("email"), s"error should list available columns; got: $msg")
  }

  test("withDefault rejects unknown column names") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import NegativeTestsSuite.User
      Table.of[User]("users").withDefault("nope")
    """)
    assert(errs.nonEmpty)
  }

  test("WHERE equality with a value of the wrong type does not compile") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.where.*
      import NegativeTestsSuite.User
      val c = ColumnsView(Table.of[User]("users").columns)
      c.age === "not an int"
    """)
    assert(errs.nonEmpty)
  }

  test("isNull on a non-nullable column does not compile with a helpful message") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.where.*
      import NegativeTestsSuite.User
      val c = ColumnsView(Table.of[User]("users").columns)
      c.age.isNull
    """)
    assert(errs.nonEmpty)
    val msg = errs.map(_.message).mkString("\n")
    assert(msg.contains("nullable columns"), s"expected friendly error mentioning nullable columns; got: $msg")
  }

  test("=== None on a nullable column does not compile (use isNull)") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.where.*
      import NegativeTestsSuite.User
      val c = ColumnsView(Table.of[User]("users").columns)
      c.deleted_at === None
    """)
    assert(errs.nonEmpty)
  }

  test("LIKE on a non-string column does not compile") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.where.*
      import NegativeTestsSuite.User
      val c = ColumnsView(Table.of[User]("users").columns)
      c.age.like("%")
    """)
    assert(errs.nonEmpty)
  }

  test("View cannot be inserted into") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val active = View.of[User]("active_users")
      active.insert((
        id = java.util.UUID.randomUUID,
        email = "x",
        age = 1,
        created_at = OffsetDateTime.now(),
        deleted_at = Option.empty[OffsetDateTime]
      ))
    """)
    assert(errs.nonEmpty)
  }

  test("View cannot be updated") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import skunk.sharp.where.*
      import NegativeTestsSuite.User
      View.of[User]("active_users").update.set(u => u.age := 1)
    """)
    assert(errs.nonEmpty)
  }

  test("View cannot be deleted from") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      View.of[User]("active_users").delete
    """)
    assert(errs.nonEmpty)
  }
}
