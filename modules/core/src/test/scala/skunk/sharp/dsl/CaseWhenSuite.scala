package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object CaseWhenSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
}

class CaseWhenSuite extends munit.FunSuite {
  import CaseWhenSuite.User

  private val users = Table.of[User]("users")

  test("caseWhen with .otherwise renders CASE WHEN … THEN … ELSE … END") {
    val af = users.select(u =>
      caseWhen(u.age < 18, lit("minor"))
        .when(u.age < 65, lit("adult"))
        .otherwise(lit("senior"))
    ).compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT CASE WHEN "age" < $1 THEN 'minor' WHEN "age" < $2 THEN 'adult' ELSE 'senior' END FROM "users""""
    )
  }

  test("caseWhen with only .end — no ELSE, result decodes as Option[T]") {
    val q: CompiledQuery[?, Option[String]] = users.select(u =>
      caseWhen(u.age < 18, lit("minor")).end
    ).compile

    assertEquals(
      q.af.fragment.sql,
      """SELECT CASE WHEN "age" < $1 THEN 'minor' END FROM "users""""
    )
  }

  test("caseWhen usable in WHERE — renders as a boolean expression at the filter") {
    val af = users.select(u => u.email)
      .where(u =>
        skunk.sharp.where.Where(
          caseWhen(u.age < 18, lit(false))
            .otherwise(lit(true))
        )
      )
      .compile.af

    assert(af.fragment.sql.contains("""WHERE CASE WHEN "age" < $1 THEN FALSE ELSE TRUE END"""), af.fragment.sql)
  }

  test("caseWhen usable in ORDER BY") {
    val af = users.select(u => u.email)
      .orderBy(u =>
        caseWhen(u.age < 18, lit(0)).otherwise(lit(1)).asc
      )
      .compile.af

    assert(af.fragment.sql.contains("""ORDER BY CASE WHEN "age" < $1 THEN 0 ELSE 1 END ASC"""), af.fragment.sql)
  }

  test("mismatched branch types fail at compile time") {
    val errs = compiletime.testing.typeCheckErrors("""
      import skunk.sharp.dsl.*
      val users = Table.of[CaseWhenSuite.User]("users")
      users.select(u =>
        caseWhen(u.age < 18, lit("minor"))
          .when(u.age < 65, lit(42))   // Int branch against a String-typed case
          .otherwise(lit("senior"))
      )
    """)
    assert(errs.nonEmpty, "expected compile error for mismatched branch types")
  }

  test(".end decoded type IS Option[T], .otherwise is T") {
    // Type-level assertion — no runtime content needed.
    val withElse: CompiledQuery[?, String] =
      users.select(u => caseWhen(u.age < 18, lit("a")).otherwise(lit("b"))).compile
    val withoutElse: CompiledQuery[?, Option[String]] =
      users.select(u => caseWhen(u.age < 18, lit("a")).end).compile
    val _ = (withElse, withoutElse)
  }
}
