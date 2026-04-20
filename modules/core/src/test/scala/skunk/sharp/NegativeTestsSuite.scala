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

  // ---- .onConflict compile-time evidence (HasUniqueness) -------------------------------------

  test(".onConflict accepts a column declared .withPrimary") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withPrimary("id")
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflict(u => u.id)
        .doNothing
        .compile
    """)
    assert(errs.isEmpty, s"expected no errors for PK column target; got: ${errs.map(_.message).mkString("\n")}")
  }

  test(".onConflict accepts a column declared .withUnique") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withUnique("email")
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflict(u => u.email)
        .doNothing
        .compile
    """)
    assert(errs.isEmpty, s"expected no errors for UNIQUE column target; got: ${errs.map(_.message).mkString("\n")}")
  }

  test(".onConflict rejects a column that is neither primary nor unique") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withPrimary("id").withUnique("email")
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflict(u => u.age)
        .doNothing
        .compile
    """)
    assert(errs.nonEmpty, "expected a compile error for non-unique column target")
  }

  test(".onConflict rejects when no constraints have been declared at all") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users") // no .withPrimary / .withUnique
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflict(u => u.id)
        .doNothing
        .compile
    """)
    assert(errs.nonEmpty, "expected a compile error when the table has no declared constraints")
  }

  // ---- Composite .onConflict evidence (HasCompositeUniqueness) -------------------------------

  test(".onConflict rejects a single-column target when the column is part of a composite PK") {
    // Being part of a composite PK doesn't qualify a column for single-column `.onConflict`. Postgres requires the
    // target to match an entire constraint.
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withCompositePrimary[("id", "email")]
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflict(u => u.id)   // partial PK — should be rejected
        .doNothing
        .compile
    """)
    assert(errs.nonEmpty, "expected rejection for single-col onConflict on a composite-PK column")
  }

  test(".onConflictComposite accepts the full composite-PK tuple") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withCompositePrimary[("id", "email")]
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflictComposite(u => (u.id, u.email))
        .doNothing
        .compile
    """)
    assert(errs.isEmpty, s"expected no errors for full composite PK target; got: ${errs.map(_.message).mkString("\n")}")
  }

  test(".onConflictComposite accepts columns in any order (set-equality)") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withCompositePrimary[("id", "email")]
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflictComposite(u => (u.email, u.id))   // reversed order — should still compile
        .doNothing
        .compile
    """)
    assert(errs.isEmpty, s"expected no errors for reversed composite PK target; got: ${errs.map(_.message).mkString("\n")}")
  }

  test(".onConflictComposite rejects a tuple that doesn't exactly match a declared composite group") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users").withCompositePrimary[("id", "email")]
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflictComposite(u => (u.id, u.age))   // "age" is not part of any group
        .doNothing
        .compile
    """)
    assert(errs.nonEmpty, "expected rejection for onConflictComposite with a non-matching column set")
  }

  test(".onConflictComposite accepts a named composite unique index") {
    val errs = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      import java.time.OffsetDateTime
      val users = Table.of[User]("users")
        .withPrimary("id")
        .withUniqueIndex["uq_email_age", ("email", "age")]
      users
        .insert((
          id = java.util.UUID.randomUUID,
          email = "x",
          age = 1,
          created_at = OffsetDateTime.now(),
          deleted_at = Option.empty[OffsetDateTime]
        ))
        .onConflictComposite(u => (u.email, u.age))
        .doNothing
        .compile
    """)
    assert(errs.isEmpty, s"expected no errors for named composite unique target; got: ${errs.map(_.message).mkString("\n")}")
  }
}
