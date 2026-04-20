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
    assert(
      errs.isEmpty,
      s"expected no errors for reversed composite PK target; got: ${errs.map(_.message).mkString("\n")}"
    )
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
    assert(
      errs.isEmpty,
      s"expected no errors for named composite unique target; got: ${errs.map(_.message).mkString("\n")}"
    )
  }

  // ---- GROUP BY coverage (GroupCoverage) -----------------------------------------------------

  test("GROUP BY coverage — projection with only aggregates compiles without GROUP BY") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.select(_ => Pg.countAll).compile
    """)
    assert(errs.isEmpty, s"pure-aggregate projection should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  test("GROUP BY coverage — bare column in projection with matching GROUP BY compiles") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.select(u => (u.age, Pg.count(u.id))).groupBy(u => u.age).compile
    """)
    assert(errs.isEmpty, s"covered projection should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  test("GROUP BY coverage — bare column in projection WITHOUT GROUP BY rejects") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.select(u => (u.age, Pg.count(u.id))).groupBy(u => u.email).compile
    """)
    assert(errs.nonEmpty, "projecting a bare column not in GROUP BY should be a compile error")
  }

  test("GROUP BY coverage — multi-column GROUP BY covers multi-column projection") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users
        .select(u => (u.age, u.email, Pg.count(u.id)))
        .groupBy(u => (u.age, u.email))
        .compile
    """)
    assert(
      errs.isEmpty,
      s"multi-col GROUP BY should cover multi-col projection; got: ${errs.map(_.message).mkString("\n")}"
    )
  }

  test("GROUP BY coverage — partial coverage (GROUP BY misses one projection column) rejects") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users
        .select(u => (u.age, u.email, Pg.count(u.id)))
        .groupBy(u => u.age)
        .compile
    """)
    assert(errs.nonEmpty, "GROUP BY that covers only some of the bare columns should fail")
  }

  test("GROUP BY coverage — aggregates (Pg.count, Pg.sum) skip coverage requirement") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users
        .select(u => (u.age, Pg.count(u.id), Pg.sum(u.age), Pg.max(u.email)))
        .groupBy(u => u.age)
        .compile
    """)
    assert(
      errs.isEmpty,
      s"aggregates should be free of coverage requirement; got: ${errs.map(_.message).mkString("\n")}"
    )
  }

  test("GROUP BY coverage — aliased expressions are free of coverage") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users
        .select(u => (u.age, Pg.count(u.id).as("cnt")))
        .groupBy(u => u.age)
        .compile
    """)
    assert(errs.isEmpty, s"aliased aggregates should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  // ---- Set-op row compatibility (AsSubquery on both sides of .union / .intersect / .except) --

  test("UNION accepts two whole-row selects of the same relation") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.select.union(users.select).compile
    """)
    assert(errs.isEmpty, s"row-compatible UNION should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  test("UNION rejects arms with different projection shapes") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      // Left projects (email), right projects (email, age) — row-incompatible.
      users.select(u => u.email).union(users.select(u => (u.email, u.age))).compile
    """)
    assert(errs.nonEmpty, "UNION with mismatched projection shapes should not compile")
  }

  // ---- Join alias-distinctness (AliasNotUsed) ------------------------------------------------

  test("self-join with two implicit aliases is a compile error — force .alias() on at least one side") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.innerJoin(users).on(r => r.users.id ==== r.users.id)
    """)
    assert(errs.nonEmpty, "self-join with matching implicit aliases should be rejected")
    val msg = errs.map(_.message).mkString("\n")
    assert(
      msg.contains("already in use") || msg.contains("AliasNotUsed"),
      s"error should mention the alias collision; got: $msg"
    )
  }

  test("self-join with distinct explicit aliases compiles") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.alias("u1").innerJoin(users.alias("u2")).on(r => r.u1.id ==== r.u2.id)
    """)
    assert(errs.isEmpty, s"distinct-alias self-join should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  test("mixing one implicit alias with an explicit distinct alias compiles") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import NegativeTestsSuite.User
      val users = Table.of[User]("users")
      users.innerJoin(users.alias("u2")).on(r => r.users.id ==== r.u2.id)
    """)
    assert(errs.isEmpty, s"implicit + distinct explicit should compile; got: ${errs.map(_.message).mkString("\n")}")
  }

  test("three-way join catches a collision against any earlier source, not just the previous one") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      import NegativeTestsSuite.User
      case class Post(id: UUID, user_id: UUID, title: String)
      val users = Table.of[User]("users")
      val posts = Table.of[Post]("posts")
      users
        .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
        .innerJoin(users).on(r => r.users.id ==== r.posts.user_id)   // third source collides with the first
    """)
    assert(errs.nonEmpty, "alias collision against an earlier source (not just the previous) should be rejected")
  }
}
