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

  // Exercises composite PK + named composite UNIQUE + single-column UNIQUE. Matches V7__orders.sql.
  case class Order(tenant_id: UUID, order_id: Long, slug: String, external_id: String, created_at: OffsetDateTime)
}

class SchemaValidatorSuite extends PgFixture {
  import SchemaValidatorSuite.*

  private val users  = Table.of[User]("users").withPrimary("id").withUnique("email")
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

  // ---- PK / UNIQUE constraint diffs -------------------------------------------------------------

  test("validate accepts matching PRIMARY KEY + UNIQUE declarations (users has PK id + UNIQUE email)") {
    withContainers { containers =>
      session(containers).use { s =>
        val usersWithConstraints = Table.of[User]("users").withPrimary("id").withUnique("email")
        SchemaValidator.validate[IO](s, usersWithConstraints).map { report =>
          assert(report.isValid, s"expected valid, got: ${report.mismatches.map(_.pretty).mkString("; ")}")
        }
      }
    }
  }

  test("validate flags PrimaryKeyColumnsDiffer when the declared PK column is wrong") {
    withContainers { containers =>
      session(containers).use { s =>
        val wrongPk = Table.of[User]("users").withPrimary("email") // DB's PK is "id"
        SchemaValidator.validate[IO](s, wrongPk).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.PrimaryKeyColumnsDiffer(_, exp, act) => exp == Set("email") && act == Set("id")
              case _                                             => false
            },
            s"expected PrimaryKeyColumnsDiffer, got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("validate flags UniqueConstraintMissing when .withUnique points at a non-unique column") {
    withContainers { containers =>
      session(containers).use { s =>
        val wrongUnique = Table.of[User]("users").withPrimary("id").withUnique("age") // "age" has no unique
        SchemaValidator.validate[IO](s, wrongUnique).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.UniqueConstraintMissing(_, "age", cols) => cols == Set("age")
              case _                                                => false
            },
            s"expected UniqueConstraintMissing(age), got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("validate flags ExtraUniqueConstraint when DB has a unique the declaration misses") {
    withContainers { containers =>
      session(containers).use { s =>
        // Declare PK only; DB also has UNIQUE on email. Expect ExtraUniqueConstraint(email).
        val pkOnly = Table.of[User]("users").withPrimary("id")
        SchemaValidator.validate[IO](s, pkOnly).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.ExtraUniqueConstraint(_, _, cols) => cols == Set("email")
              case _                                          => false
            },
            s"expected ExtraUniqueConstraint(email), got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("validate flags ExtraPrimaryKey when DB has a PK but nothing is declared") {
    withContainers { containers =>
      session(containers).use { s =>
        val noPkDecl = Table.of[User]("users").withUnique("email") // PK "id" undeclared
        SchemaValidator.validate[IO](s, noPkDecl).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.ExtraPrimaryKey(_, actual) => actual == Set("id")
              case _                                   => false
            },
            s"expected ExtraPrimaryKey(id), got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  // ---- Composite constraints (PR: composite PK + composite UNIQUE) --------------------------------

  test("validate accepts a composite PK + composite UNIQUE + single-column UNIQUE declaration") {
    withContainers { containers =>
      session(containers).use { s =>
        val orders = Table.of[Order]("orders")
          .withCompositePrimary[("tenant_id", "order_id")]
          .withUniqueIndex["uq_orders_tenant_slug", ("tenant_id", "slug")]
          .withUnique("external_id")
          .withDefault("created_at")

        SchemaValidator.validate[IO](s, orders).map { report =>
          assert(report.isValid, s"expected valid, got: ${report.mismatches.map(_.pretty).mkString("; ")}")
        }
      }
    }
  }

  test("validate flags a mis-declared composite PK (wrong column set)") {
    withContainers { containers =>
      session(containers).use { s =>
        // DB PK is (tenant_id, order_id). Declare (tenant_id, slug) instead — expect PrimaryKeyColumnsDiffer.
        val wrongPk = Table.of[Order]("orders").withCompositePrimary[("tenant_id", "slug")]
        SchemaValidator.validate[IO](s, wrongPk).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.PrimaryKeyColumnsDiffer(_, exp, act) =>
                exp == Set("tenant_id", "slug") && act == Set("tenant_id", "order_id")
              case _ => false
            },
            s"expected PrimaryKeyColumnsDiffer, got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("validate flags a missing composite UNIQUE when declared but absent in DB") {
    withContainers { containers =>
      session(containers).use { s =>
        // Declare a composite unique on (slug, external_id) — doesn't exist in DB.
        val orders = Table.of[Order]("orders")
          .withCompositePrimary[("tenant_id", "order_id")]
          .withUniqueIndex["uq_bogus", ("slug", "external_id")]
        SchemaValidator.validate[IO](s, orders).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.UniqueConstraintMissing(_, "uq_bogus", cols) => cols == Set("slug", "external_id")
              case _                                                     => false
            },
            s"expected UniqueConstraintMissing(uq_bogus), got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("validate flags ExtraUniqueConstraint for a DB composite unique the declaration misses") {
    withContainers { containers =>
      session(containers).use { s =>
        // Declare only PK + external_id unique — miss the (tenant_id, slug) composite. Expect ExtraUniqueConstraint.
        val orders = Table.of[Order]("orders")
          .withCompositePrimary[("tenant_id", "order_id")]
          .withUnique("external_id")
        SchemaValidator.validate[IO](s, orders).map { report =>
          assert(
            report.mismatches.exists {
              case Mismatch.ExtraUniqueConstraint(_, _, cols) => cols == Set("tenant_id", "slug")
              case _                                          => false
            },
            s"expected ExtraUniqueConstraint on (tenant_id, slug), got: ${report.mismatches.map(_.pretty).mkString("; ")}"
          )
        }
      }
    }
  }

  test("constraints aren't checked on views (Postgres doesn't allow PK / UNIQUE on views)") {
    withContainers { containers =>
      session(containers).use { s =>
        // `active_users` is a VIEW — even if we declared .withPrimary on it, the validator shouldn't produce
        // any constraint-related mismatches for it (the constraint query is skipped for views entirely).
        SchemaValidator.validate[IO](s, active).map { report =>
          val constraintKinds = Set(
            "PrimaryKeyMissing",
            "PrimaryKeyColumnsDiffer",
            "ExtraPrimaryKey",
            "UniqueConstraintMissing",
            "ExtraUniqueConstraint"
          )
          val constraintMismatches = report.mismatches.filter(m => constraintKinds.contains(m.getClass.getSimpleName))
          assert(constraintMismatches.isEmpty, s"unexpected constraint mismatches on view: $constraintMismatches")
        }
      }
    }
  }
}
