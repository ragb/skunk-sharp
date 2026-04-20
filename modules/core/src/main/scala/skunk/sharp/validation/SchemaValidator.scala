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

import cats.effect.Concurrent
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import skunk.sharp.{Column, Relation, Table}
import skunk.sharp.pg.PgTypes

/**
 * Compare declared [[Relation]]s against a live Postgres catalogue.
 *
 * Two entry points:
 *
 *   - [[validate]] — report-only: returns every mismatch in a [[ValidationReport]]. Callers decide how to react.
 *   - [[validateOrRaise]] — fail-fast: raises [[SchemaValidationException]] if anything is wrong. Built on top of
 *     `validate` so you always have the structured report available at the catch site.
 *
 * Checks performed per relation:
 *   - Existence (`information_schema.tables`).
 *   - Kind: `BASE TABLE` vs `VIEW` (matches [[Relation.expectedTableType]]).
 *   - Every declared column is present in `information_schema.columns` with the expected `data_type` and nullability.
 *   - Any extra columns in the database that the declaration doesn't know about (reported so callers can decide whether
 *     to treat drift strictly).
 */
object SchemaValidator {

  private case class ColumnInfo(
    name: String,
    dataType: String,
    isNullable: Boolean,
    charMaxLength: Option[Int],
    numericPrecision: Option[Int],
    numericScale: Option[Int]
  )

  private val relationKindQuery: Query[(String, String), String] =
    sql"""
      SELECT table_type::text
      FROM information_schema.tables
      WHERE table_schema = $varchar
        AND table_name = $varchar
    """.query(text)

  private val columnsQuery: Query[(String, String), ColumnInfo] =
    sql"""
      SELECT column_name::text,
             data_type::text,
             is_nullable::text,
             character_maximum_length,
             numeric_precision,
             numeric_scale
      FROM information_schema.columns
      WHERE table_schema = $varchar
        AND table_name = $varchar
      ORDER BY ordinal_position
    """.query(text *: text *: text *: int4.opt *: int4.opt *: int4.opt).map {
      case (n, dt, nullableStr, cml, np, ns) =>
        ColumnInfo(n, dt, nullableStr.equalsIgnoreCase("YES"), cml, np, ns)
    }

  /**
   * One `(constraint_type, constraint_name, column_name)` row per column participating in a constraint. We aggregate in
   * Scala: constraints group by name, and multi-column keys become a set per constraint. Covers PRIMARY KEY and UNIQUE;
   * FOREIGN KEY / CHECK aren't validated (not declarable in the Scala description today).
   */
  private case class ConstraintRow(kind: String, name: String, column: String)

  private val constraintsQuery: Query[(String, String), ConstraintRow] =
    sql"""
      SELECT tc.constraint_type::text,
             tc.constraint_name::text,
             kcu.column_name::text
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_schema = kcu.constraint_schema
       AND tc.constraint_name   = kcu.constraint_name
      WHERE tc.table_schema = $varchar
        AND tc.table_name   = $varchar
        AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
      ORDER BY tc.constraint_name, kcu.ordinal_position
    """.query(text *: text *: text).map((k, n, c) => ConstraintRow(k, n, c))

  /** Report-only primitive: returns every mismatch the declared relations have with the live database. */
  def validate[F[_]: Concurrent](session: Session[F], relations: Relation[?]*): F[ValidationReport] =
    relations.toList.traverse(validateOne(session, _)).map(reports => ValidationReport(reports.flatMap(_.mismatches)))

  /** Fail-fast helper: raises [[SchemaValidationException]] if any mismatches are found. */
  def validateOrRaise[F[_]: Concurrent](session: Session[F], relations: Relation[?]*): F[Unit] =
    validate(session, relations*).flatMap { report =>
      if report.isValid then ().pure[F]
      else Concurrent[F].raiseError(new SchemaValidationException(report))
    }

  private def validateOne[F[_]: Concurrent](session: Session[F], relation: Relation[?]): F[ValidationReport] = {
    // Derived relations (subquery-as-relation, VALUES, set-returning functions) aren't registered in
    // `information_schema` — skip them. They carry an empty `expectedTableType` as the marker.
    if (relation.expectedTableType.isEmpty) return ValidationReport.empty.pure[F]
    val schema = relation.schema.getOrElse("public")
    val label  = relation.qualifiedName
    for {
      kindsList <- session.prepare(relationKindQuery).flatMap(_.stream((schema, relation.name), 32).compile.toList)
      kinds = kindsList: List[String]
      report <- kinds match {
        case Nil =>
          (ValidationReport(List(Mismatch.RelationMissing(
            label,
            relation.expectedTableType
          ))): ValidationReport).pure[F]
        case actual :: _ if actual != relation.expectedTableType =>
          (ValidationReport(List(Mismatch.RelationKindMismatch(
            label,
            relation.expectedTableType,
            actual
          ))): ValidationReport)
            .pure[F]
        case _ =>
          for {
            actualCols <- session.prepare(columnsQuery).flatMap(_.stream((schema, relation.name), 64).compile.toList)
            columnReport = diffColumns(label, relation, actualCols)
            constraintReport <-
              if (relation.expectedTableType == "VIEW") ValidationReport.empty.pure[F]
              else
                session
                  .prepare(constraintsQuery)
                  .flatMap(_.stream((schema, relation.name), 64).compile.toList)
                  .map(rows => diffConstraints(label, relation, rows))
          } yield columnReport ++ constraintReport
      }
    } yield report
  }

  /**
   * Compare declared primary-key and unique-constraint data against the database. Handles both single-column and
   * composite constraints:
   *
   *   - PK: set-based comparison of columns (ordering inside a composite PK isn't declarable today).
   *   - UNIQUE: keyed by constraint name on both sides, so a declared `.withUnique("email")` (auto-name `"email"`) and
   *     a `.withUniqueIndex("uq_tenant_slug", ("tenant_id", "slug"))` are diffed separately and independently.
   */
  private def diffConstraints(
    label: String,
    relation: Relation[?],
    rows: List[ConstraintRow]
  ): ValidationReport = {
    val cols                        = relation.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val declaredPkCols: Set[String] = cols.filter(_.isPrimary).map(_.name: String).toSet

    // Declared unique constraints, keyed by constraint name.
    val declaredUniques: Map[String, Set[String]] = relation match {
      case t: Table[?, ?] => t.uniqueIndexes.view.mapValues(_.toSet).toMap
      case _              => Map.empty
    }

    // Group DB constraint rows by constraint name, then bucket by kind.
    val byConstraint: Map[(String, String), Set[String]] =
      rows.groupMap(r => (r.kind, r.name))(_.column).view.mapValues(_.toSet).toMap
    val actualPkColsOpt: Option[Set[String]] =
      byConstraint.collectFirst { case ((kind, _), cs) if kind == "PRIMARY KEY" => cs }
    val actualUniques: Map[String, Set[String]] =
      byConstraint.iterator.collect { case ((kind, name), cs) if kind == "UNIQUE" => name -> cs }.toMap

    val pkMismatches: List[Mismatch] = (declaredPkCols.isEmpty, actualPkColsOpt) match {
      case (true, None)                                      => Nil
      case (true, Some(actual))                              => List(Mismatch.ExtraPrimaryKey(label, actual))
      case (false, None)                                     => List(Mismatch.PrimaryKeyMissing(label, declaredPkCols))
      case (false, Some(actual)) if actual != declaredPkCols =>
        List(Mismatch.PrimaryKeyColumnsDiffer(label, declaredPkCols, actual))
      case _ => Nil
    }

    // Match UNIQUE constraints by their column set, not by name — Postgres auto-generates names for inline
    // `UNIQUE` columns (`<table>_<col>_key`) while the DSL defaults to the user-facing column name for the
    // `.withUnique(col)` shorthand. Matching by column set (which is unambiguous, since Postgres forbids two unique
    // constraints on the same set of columns) gracefully handles both. We still carry the declared or DB name through
    // so `Mismatch` messages are identifiable.
    val declaredByCols: Map[Set[String], String] =
      declaredUniques.iterator.map { case (n, cs) => cs -> n }.toMap
    val actualByCols: Map[Set[String], String] =
      actualUniques.iterator.map { case (n, cs) => cs -> n }.toMap

    val uniqueMismatches: List[Mismatch] = {
      val missing = (declaredByCols.keySet -- actualByCols.keySet).toList
        .sortBy(_.toList.sorted.mkString(","))
        .map(cs => Mismatch.UniqueConstraintMissing(label, declaredByCols(cs), cs))
      val extra = (actualByCols.keySet -- declaredByCols.keySet).toList
        .sortBy(_.toList.sorted.mkString(","))
        .map(cs => Mismatch.ExtraUniqueConstraint(label, actualByCols(cs), cs))
      missing ++ extra
    }

    ValidationReport(pkMismatches ++ uniqueMismatches)
  }

  private def diffColumns(label: String, relation: Relation[?], actual: List[ColumnInfo]): ValidationReport = {
    val declared = relation.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val byName   = actual.map(c => c.name -> c).toMap

    // Postgres reports every view column as nullable in information_schema regardless of the underlying base-table
    // constraints — the planner can't prove non-nullability through a view. Skip the nullability check for views so
    // declaring `Option` vs non-`Option` is a Scala-side modelling choice, not a catalogue-level truth.
    val skipNullability = relation.expectedTableType == "VIEW"

    val declaredMismatches = declared.flatMap { col =>
      byName.get(col.name) match {
        case None =>
          List(Mismatch.ColumnMissing(label, col.name))
        case Some(info) =>
          // Reconstruct the actual column type in skunk's short form (e.g. "varchar(256)") from
          // `data_type` + `character_maximum_length` / numeric precision / scale. Compare to the declared
          // `skunk.data.Type`'s name, which already carries parameters for parametric types.
          val expected = col.tpe.name
          val actual   =
            PgTypes.actualTypeName(info.dataType, info.charMaxLength, info.numericPrecision, info.numericScale)
          val typeIssue =
            Option.when(expected != actual)(
              Mismatch.TypeMismatch(label, col.name, expected, actual)
            )
          val nullIssue =
            Option.when(!skipNullability && info.isNullable != col.isNullable)(
              Mismatch.NullabilityMismatch(label, col.name, col.isNullable, info.isNullable)
            )
          typeIssue.toList ++ nullIssue.toList
      }
    }

    val declaredNames   = declared.map(_.name).toSet
    val extraMismatches =
      actual.map(_.name).filterNot(declaredNames.contains).map(Mismatch.ExtraColumn(label, _))

    ValidationReport(declaredMismatches ++ extraMismatches)
  }

}
