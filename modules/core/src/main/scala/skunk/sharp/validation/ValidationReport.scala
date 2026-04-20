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

  /** Declared `.withUnique(column)` but no matching single-column `UNIQUE` constraint in the database. */
  final case class UniqueConstraintMissing(relation: String, column: String) extends Mismatch {
    def pretty = s"relation $relation: expected a unique constraint on $column but none found in the database"
  }

  /**
   * Database has a single-column unique constraint on a column that isn't declared `.withUnique`. Usually informational
   * (an index-backed unique that the Scala description doesn't know about); report so callers see it.
   */
  final case class ExtraUniqueConstraint(relation: String, column: String) extends Mismatch {
    def pretty = s"relation $relation: DB has a unique constraint on $column that is not declared"
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
