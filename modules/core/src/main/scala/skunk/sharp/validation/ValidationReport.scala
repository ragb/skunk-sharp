/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package skunk.sharp.validation

/** One discrepancy between a declared [[skunk.sharp.Relation]] and its live Postgres counterpart. */
sealed trait Mismatch {
  def relation: String
  def pretty: String
}

object Mismatch {

  final case class RelationMissing(relation: String, expectedKind: String) extends Mismatch {
    def pretty = s"relation $relation is declared but not found in the database (expected kind: $expectedKind)"
  }

  final case class RelationKindMismatch(relation: String, expectedKind: String, actualKind: String) extends Mismatch {
    def pretty = s"relation $relation: expected $expectedKind, got $actualKind"
  }

  final case class ColumnMissing(relation: String, column: String) extends Mismatch {
    def pretty = s"relation $relation: column $column is declared but not found in the database"
  }

  final case class ExtraColumn(relation: String, column: String) extends Mismatch {
    def pretty = s"relation $relation: column $column exists in the database but is not declared"
  }

  final case class TypeMismatch(relation: String, column: String, expected: String, actual: String) extends Mismatch {
    def pretty = s"relation $relation: column $column expected Postgres type '$expected' but found '$actual'"
  }

  final case class NullabilityMismatch(
    relation: String,
    column: String,
    expectedNullable: Boolean,
    actualNullable: Boolean
  ) extends Mismatch {

    def pretty =
      s"relation $relation: column $column expected nullable=$expectedNullable but database has nullable=$actualNullable"

  }

  /** Declared primary-key column(s) found, but the database has no primary key at all on this relation. */
  final case class PrimaryKeyMissing(relation: String, expected: Set[String]) extends Mismatch {
    def pretty = s"relation $relation: expected primary key on ${expected.mkString("(", ", ", ")")} but DB has none"
  }

  /**
   * Declared primary-key column set differs from the database's. Matching is set-based — ordering inside a composite PK
   * isn't tracked at declaration time today, so `(a, b)` declared vs `(b, a)` in DB is not reported.
   */
  final case class PrimaryKeyColumnsDiffer(
    relation: String,
    expected: Set[String],
    actual: Set[String]
  ) extends Mismatch {

    def pretty =
      s"relation $relation: expected primary key on ${expected.mkString("(", ", ", ")")} but DB has " +
        actual.mkString("(", ", ", ")")

  }

  /**
   * Database has a primary key but nothing in the Scala description flags one. Usually a declaration gap — reported so
   * callers can decide whether to tighten the description.
   */
  final case class ExtraPrimaryKey(relation: String, actual: Set[String]) extends Mismatch {

    def pretty =
      s"relation $relation: DB has a primary key on ${actual.mkString("(", ", ", ")")} but nothing is declared"

  }

  /**
   * Declared `.withUnique(column)` / `.withUniqueIndex(name, cols)` but no matching `UNIQUE` constraint in the
   * database. `name` is the constraint name (matches the DB's `constraint_name`); `columns` is the full column set.
   * Single-column unique constraints use the column name as `name`, matching Postgres's default naming convention.
   */
  final case class UniqueConstraintMissing(relation: String, name: String, columns: Set[String]) extends Mismatch {

    def pretty =
      s"relation $relation: expected unique constraint \"$name\" on ${columns.mkString("(", ", ", ")")} but none found " +
        "in the database"

  }

  /**
   * Database has a `UNIQUE` constraint that the Scala description doesn't declare (by name). Usually informational;
   * report so callers can tighten their declaration. `name` is the DB's `constraint_name`, `columns` its column set.
   */
  final case class ExtraUniqueConstraint(relation: String, name: String, columns: Set[String]) extends Mismatch {

    def pretty =
      s"relation $relation: DB has unique constraint \"$name\" on ${columns.mkString("(", ", ", ")")} that is not " +
        "declared"

  }

}

/** Result of comparing one-or-more declared relations to a live Postgres schema. */
final case class ValidationReport(mismatches: List[Mismatch]) {
  def isValid: Boolean                              = mismatches.isEmpty
  def ++(other: ValidationReport): ValidationReport = ValidationReport(mismatches ++ other.mismatches)
}

object ValidationReport {
  val empty: ValidationReport = ValidationReport(Nil)
}

/**
 * Thrown by `SchemaValidator.validateOrRaise` when the declared schema diverges from the live database. Carries the
 * full [[ValidationReport]] for callers that want to inspect the individual mismatches.
 */
final class SchemaValidationException(val report: ValidationReport)
    extends RuntimeException(s"skunk-sharp schema validation failed with ${report.mismatches.size} mismatch(es):\n" +
      report.mismatches.map("  - " + _.pretty).mkString("\n"))
