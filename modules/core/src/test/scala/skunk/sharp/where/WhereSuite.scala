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
    val w = cols.email === lit("a@b")
    assertEquals(w.fragment.sql.trim, """"email" = 'a@b'""")
  }

  test("AND composes two predicates") {
    val w = (cols.email === lit("a@b")) && (cols.age >= lit(18))
    assertEquals(w.fragment.sql, """("email" = 'a@b' AND "age" >= 18)""")
  }

  test("OR / NOT compose") {
    val w = !(cols.age < lit(18) || cols.age > lit(100))
    assertEquals(w.fragment.sql, """NOT (("age" < 18 OR "age" > 100))""")
  }

  test("IN renders comma-separated placeholders (from NonEmptyList)") {
    val w = cols.age.in(cats.data.NonEmptyList.of(1, 2, 3))
    assertEquals(w.fragment.sql, """"age" IN ($1, $2, $3)""")
  }

  test("IN accepts any cats.Reducible container (NonEmptyVector here)") {
    val w = cols.age.in(cats.data.NonEmptyVector.of(1, 2))
    assertEquals(w.fragment.sql, """"age" IN ($1, $2)""")
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
    val w = cols.email.like(lit("%@example.com"))
    assertEquals(w.fragment.sql.trim, """"email" LIKE '%@example.com'""")
  }

  test("isNull available only on nullable columns") {
    val w = cols.deleted_at.isNull
    assertEquals(w.fragment.sql, """"deleted_at" IS NULL""")
  }

  test("equality on a nullable column takes the Option-wrapped value") {
    val ts = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    // cols.deleted_at: TypedColumn[Option[OffsetDateTime], true].
    // The value-RHS overload no longer auto-strips Option (`Stripped[T]` removed for overload-resolution
    // reasons — see ExprOps); pass `Some(value)` / `None` directly. Most callers use `.isNull` /
    // `.isNotNull` for null-checks anyway.
    val w = cols.deleted_at === Param.bind(Option(ts))
    assertEquals(w.fragment.sql.trim, """"deleted_at" = $1""")
  }

  test("BETWEEN with literal bounds renders the bounds inline") {
    val w = cols.age.between(lit(18), lit(65))
    assertEquals(w.fragment.sql, """"age" BETWEEN 18 AND 65""")
  }

  test("NOT BETWEEN renders the keyword form, not the expanded AND") {
    val w = cols.age.notBetween(lit(18), lit(65))
    assertEquals(w.fragment.sql, """"age" NOT BETWEEN 18 AND 65""")
  }

  test("BETWEEN SYMMETRIC — Postgres auto-swap form") {
    val w = cols.age.betweenSymmetric(lit(100), lit(0))
    assertEquals(w.fragment.sql, """"age" BETWEEN SYMMETRIC 100 AND 0""")
  }

  test("IS DISTINCT FROM — NULL-safe inequality on a nullable column") {
    val ts = OffsetDateTime.parse("2020-01-01T00:00:00Z")
    val w  = cols.deleted_at.isDistinctFrom(Param.bind(Some(ts)))
    assertEquals(w.fragment.sql, """"deleted_at" IS DISTINCT FROM $1""")
  }

  test("IS NOT DISTINCT FROM — NULL-safe equality") {
    val w = cols.age.isNotDistinctFrom(lit(42))
    assertEquals(w.fragment.sql, """"age" IS NOT DISTINCT FROM 42""")
  }

  test("SIMILAR TO renders the Postgres regex-ish form") {
    val w = cols.email.similarTo(lit("[a-z]+@[a-z]+"))
    assertEquals(w.fragment.sql, """"email" SIMILAR TO '[a-z]+@[a-z]+'""")
  }

  test("NOT SIMILAR TO") {
    val w = cols.email.notSimilarTo(lit("%.test"))
    assertEquals(w.fragment.sql, """"email" NOT SIMILAR TO '%.test'""")
  }

  test("`=== None` on a nullable column is a compile error — point users at `.isNull`") {
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
    assert(result.nonEmpty, "expected `=== None` to be a compile error; the supported form is `.isNull`")
  }
}
