package skunk.sharp.where

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object WhereSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class WhereSuite extends munit.FunSuite {
  import WhereSuite.User

  private val users = Table.of[User]("users")
  private val cols  = ColumnsView(users.columns)

  test("equality renders correctly") {
    val w = cols.email === "a@b"
    assertEquals(w.render.fragment.sql.trim, """"email" = $1""")
  }

  test("AND composes two predicates") {
    val w = (cols.email === "a@b") && (cols.age >= 18)
    assertEquals(w.render.fragment.sql, """("email" = $1 AND "age" >= $2)""")
  }

  test("OR / NOT compose") {
    val w = !(cols.age < 18 || cols.age > 100)
    assertEquals(w.render.fragment.sql, """NOT (("age" < $1 OR "age" > $2))""")
  }

  test("IN renders comma-separated placeholders (from NonEmptyList)") {
    val w = cols.age.in(cats.data.NonEmptyList.of(1, 2, 3))
    assertEquals(w.render.fragment.sql, """"age" IN ($1, $2, $3)""")
  }

  test("IN accepts any cats.Reducible container (NonEmptyVector here)") {
    val w = cols.age.in(cats.data.NonEmptyVector.of(1, 2))
    assertEquals(w.render.fragment.sql, """"age" IN ($1, $2)""")
  }

  test("empty input is a compile error (Seq is no longer accepted; Reducible guarantees non-empty)") {
    val errs = scala.compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[WhereSuite.User]("users")
      val cv    = ColumnsView(users.columns)
      cv.age.in(Seq.empty[Int])
    """)
    assert(errs.nonEmpty, "expected a compile error for Seq on .in")
  }

  test("LIKE on string column") {
    val w = cols.email.like("%@example.com")
    assertEquals(w.render.fragment.sql.trim, """"email" LIKE $1""")
  }

  test("isNull available only on nullable columns") {
    val w = cols.deleted_at.isNull
    assertEquals(w.render.fragment.sql, """"deleted_at" IS NULL""")
  }

  test("equality on a nullable column takes the underlying value, not Option") {
    val ts = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    // cols.deleted_at: TypedColumn[Option[OffsetDateTime], true]
    // rhs must be OffsetDateTime (Stripped), never Option[OffsetDateTime].
    val w = cols.deleted_at === ts
    assertEquals(w.render.fragment.sql.trim, """"deleted_at" = $1""")
  }

  test("BETWEEN renders with two bound parameters joined by AND") {
    val w = cols.age.between(18, 65)
    assertEquals(w.render.fragment.sql, """"age" BETWEEN $1 AND $2""")
  }

  test("NOT BETWEEN renders the keyword form, not the expanded AND") {
    val w = cols.age.notBetween(18, 65)
    assertEquals(w.render.fragment.sql, """"age" NOT BETWEEN $1 AND $2""")
  }

  test("BETWEEN SYMMETRIC — Postgres auto-swap form") {
    val w = cols.age.betweenSymmetric(100, 0)
    assertEquals(w.render.fragment.sql, """"age" BETWEEN SYMMETRIC $1 AND $2""")
  }

  test("IS DISTINCT FROM — NULL-safe inequality on a nullable column") {
    val ts = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    val w  = cols.deleted_at.isDistinctFrom(ts)
    assertEquals(w.render.fragment.sql, """"deleted_at" IS DISTINCT FROM $1""")
  }

  test("IS NOT DISTINCT FROM — NULL-safe equality") {
    val w = cols.age.isNotDistinctFrom(42)
    assertEquals(w.render.fragment.sql, """"age" IS NOT DISTINCT FROM $1""")
  }

  test("SIMILAR TO renders the Postgres regex-ish form") {
    val w = cols.email.similarTo("[a-z]+@[a-z]+")
    assertEquals(w.render.fragment.sql, """"email" SIMILAR TO $1""")
  }

  test("NOT SIMILAR TO") {
    val w = cols.email.notSimilarTo("%.test")
    assertEquals(w.render.fragment.sql, """"email" NOT SIMILAR TO $1""")
  }

  test("comparing a nullable column to None/Option does NOT compile") {
    // Documents the compile-time rejection with a compiletime.testing assertion.
    import scala.compiletime.testing.*
    val result: List[Error] = typeCheckErrors("""
      import skunk.sharp.*
      import skunk.sharp.ops.*, skunk.sharp.where.*
      import WhereSuite.User
      import java.time.OffsetDateTime
      val t = Table.of[User]("users")
      val c = ColumnsView(t.columns)
      c.deleted_at === None
    """)
    assert(result.nonEmpty, "expected `=== None` on a nullable column to be a compile error")
  }
}
