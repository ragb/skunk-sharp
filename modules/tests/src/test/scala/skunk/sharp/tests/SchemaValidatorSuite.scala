package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object SchemaValidatorSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
  case class ActiveUser(id: UUID, email: String, age: Int)

  // Intentionally wrong: `emails` is not a column; `age` declared as String instead of Int; `created_at` nullability
  // claim is wrong.
  case class BrokenUser(id: UUID, emails: String, age: String, created_at: Option[OffsetDateTime])
}

class SchemaValidatorSuite extends PgFixture {
  import SchemaValidatorSuite.*

  private val users  = Table.of[User]("users")
  private val active = View.of[ActiveUser]("active_users")
  private val broken = Table.of[BrokenUser]("users")

  test("validate passes when declared schema matches the database") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, users, active).map { report =>
          assert(report.isValid, s"expected valid, got: ${report.mismatches.map(_.pretty).mkString("; ")}")
        }
      }
    }
  }

  test("validate reports type and nullability mismatches and missing columns") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, broken).map { report =>
          val kinds = report.mismatches.map(_.getClass.getSimpleName).toSet
          assert(kinds.contains("ColumnMissing"), s"expected ColumnMissing, got: $kinds")
          assert(kinds.contains("TypeMismatch"), s"expected TypeMismatch, got: $kinds")
          assert(kinds.contains("NullabilityMismatch"), s"expected NullabilityMismatch, got: $kinds")
        }
      }
    }
  }

  test("validateOrRaise succeeds when everything matches") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validateOrRaise[IO](s, users, active)
      }
    }
  }

  test("validateOrRaise raises SchemaValidationException when not") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validateOrRaise[IO](s, broken).attempt.map {
          case Left(_: SchemaValidationException) => ()
          case other                              => fail(s"expected SchemaValidationException, got $other")
        }
      }
    }
  }

  test("validate flags a relation-kind mismatch (a table declared as a view, or vice versa)") {
    withContainers { containers =>
      session(containers).use { s =>
        // Declare `users` as a View — it's actually a BASE TABLE, so we expect a RelationKindMismatch.
        val usersAsView = View.of[User]("users")
        SchemaValidator.validate[IO](s, usersAsView).map { report =>
          val kinds = report.mismatches.map(_.getClass.getSimpleName).toSet
          assert(kinds.contains("RelationKindMismatch"), s"expected RelationKindMismatch, got: $kinds")
        }
      }
    }
  }
}
