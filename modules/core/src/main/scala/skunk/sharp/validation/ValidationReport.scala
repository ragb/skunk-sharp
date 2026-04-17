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
