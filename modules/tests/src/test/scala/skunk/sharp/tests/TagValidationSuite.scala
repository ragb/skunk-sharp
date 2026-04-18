package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.*

object TagValidationSuite {
  case class Party(id: Long, name: Varchar[64], postal_code: Bpchar[5], balance: Numeric[10, 2])

  // Wrong parameters: varchar(128) when the DB has varchar(64).
  case class PartyWrongVarchar(id: Long, name: Varchar[128], postal_code: Bpchar[5], balance: Numeric[10, 2])

  // Wrong parameters: numeric(10, 3) when the DB has numeric(10, 2).
  case class PartyWrongNumeric(id: Long, name: Varchar[64], postal_code: Bpchar[5], balance: Numeric[10, 3])
}

class TagValidationSuite extends PgFixture {
  import TagValidationSuite.*

  private val parties      = Table.of[Party]("parties").withDefault("id").withDefault("balance")
  private val wrongVarchar = Table.of[PartyWrongVarchar]("parties").withDefault("id").withDefault("balance")
  private val wrongNumeric = Table.of[PartyWrongNumeric]("parties").withDefault("id").withDefault("balance")

  test("tagged declaration passes validation when lengths/precision match the DB") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, parties).map { report =>
          assert(report.isValid, s"expected valid, got: ${report.mismatches.map(_.pretty).mkString("; ")}")
        }
      }
    }
  }

  test("wrong varchar length is flagged as TypeMismatch (varchar(128) vs varchar(64))") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, wrongVarchar).map { report =>
          val mismatch = report.mismatches.collectFirst {
            case m: Mismatch.TypeMismatch if m.column == "name" => m
          }
          assert(mismatch.isDefined, s"expected TypeMismatch for 'name', got: ${report.mismatches.map(_.pretty)}")
          assertEquals(mismatch.get.expected, "varchar(128)")
          assertEquals(mismatch.get.actual, "varchar(64)")
        }
      }
    }
  }

  test("wrong numeric scale is flagged as TypeMismatch (numeric(10,3) vs numeric(10,2))") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, wrongNumeric).map { report =>
          val mismatch = report.mismatches.collectFirst {
            case m: Mismatch.TypeMismatch if m.column == "balance" => m
          }
          assert(mismatch.isDefined, s"expected TypeMismatch for 'balance', got: ${report.mismatches.map(_.pretty)}")
          assertEquals(mismatch.get.expected, "numeric(10,3)")
          assertEquals(mismatch.get.actual, "numeric(10,2)")
        }
      }
    }
  }
}
