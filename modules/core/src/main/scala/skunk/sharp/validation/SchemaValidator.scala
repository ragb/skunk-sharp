package skunk.sharp.validation

import cats.effect.Concurrent
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import skunk.sharp.{Column, IndexDef, Relation, Table}
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
    udtName: String,
    isNullable: Boolean,
    charMaxLength: Option[Int],
    numericPrecision: Option[Int],
    numericScale: Option[Int],
    isGenerated: Boolean,
    formattedType: Option[String]
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
             udt_name::text,
             is_nullable::text,
             character_maximum_length,
             numeric_precision,
             numeric_scale,
             is_generated::text,
             (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_catalog.pg_attribute a
               WHERE a.attrelid = (quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass
                 AND a.attname = column_name)::text
      FROM information_schema.columns
      WHERE table_schema = $varchar
        AND table_name = $varchar
      ORDER BY ordinal_position
    """.query(text *: text *: text *: text *: int4.opt *: int4.opt *: int4.opt *: text *: text.opt).map {
      case (n, dt, udt, nullableStr, cml, np, ns, generatedStr, formatted) =>
        ColumnInfo(
          n,
          dt,
          udt,
          nullableStr.equalsIgnoreCase("YES"),
          cml,
          np,
          ns,
          generatedStr.equalsIgnoreCase("ALWAYS"),
          formatted
        )
    }

  /**
   * One `(constraint_type, constraint_name, column_name)` row per column participating in a constraint. We aggregate in
   * Scala: constraints group by name, and multi-column keys become a set per constraint. Covers PRIMARY KEY and UNIQUE;
   * FOREIGN KEY / CHECK aren't validated (not declarable in the Scala description today).
   */
  private case class ConstraintRow(kind: String, name: String, column: String)

  /** One index of a table, as read from `pg_index` — see `indexesQuery`. */
  private case class IndexRow(
    name: String,
    unique: Boolean,
    primary: Boolean,
    method: String,
    columns: List[String],
    predicate: Option[String]
  ) {

    /** Rendered like [[IndexDef.definition]] so the two compare after normalisation. */
    def definition: String =
      (if (unique) "UNIQUE " else "") + columns.mkString(s"$method (", ", ", ")") +
        predicate.fold("")(p => s" WHERE $p")

  }

  /**
   * One row per index on a table: name, unique, primary, access method, the key columns rendered like `pg_get_indexdef`
   * does (with non-default `DESC` / `NULLS …` modifiers from `indoption`), and the partial-index predicate. Expression
   * keys come through as their expression text.
   */
  private val indexesQuery: Query[(String, String), IndexRow] =
    sql"""
      SELECT ic.relname::text,
             i.indisunique,
             i.indisprimary,
             am.amname::text,
             ARRAY(
               SELECT pg_get_indexdef(i.indexrelid, k, true) ||
                      CASE WHEN (i.indoption[k - 1]::int & 1) = 1
                           THEN CASE WHEN (i.indoption[k - 1]::int & 2) = 2 THEN ' DESC' ELSE ' DESC NULLS LAST' END
                           ELSE CASE WHEN (i.indoption[k - 1]::int & 2) = 2 THEN ' NULLS FIRST' ELSE '' END
                      END
               FROM generate_series(1, i.indnkeyatts::int) AS k
               ORDER BY k
             )::text[],
             pg_get_expr(i.indpred, i.indrelid)
      FROM pg_index i
      JOIN pg_class ic    ON ic.oid = i.indexrelid
      JOIN pg_class t     ON t.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN pg_am am       ON am.oid = ic.relam
      WHERE n.nspname = $text AND t.relname = $text
      ORDER BY ic.relname
    """.query(text *: bool *: bool *: text *: _text *: text.opt).map {
      case (name, unique, primary, method, cols, pred) =>
        IndexRow(name, unique, primary, method, cols.flattenTo(List), pred)
    }

  /** Names of every extension currently installed in the connected database (from `pg_extension`). */
  private val extensionsQuery: Query[Void, String] =
    sql"SELECT extname::text FROM pg_extension".query(text)

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

  /**
   * Report-only primitive: returns every mismatch the declared relations have with the live database.
   *
   * Postgres extensions required by columns whose `PgTypeFor` carries a `requiredExtension` (citext, ltree, hstore, …)
   * are collected automatically from `relations`. `extraExtensions` opts in additional names for function-only contribs
   * (pgcrypto, fuzzystrmatch) that don't appear in any column.
   */
  def validate[F[_]: Concurrent](
    session: Session[F],
    relations: Seq[Relation[?]],
    extraExtensions: Set[String]
  ): F[ValidationReport] = {
    val required: Set[String] =
      relations.iterator.flatMap(_.requiredExtensions).toSet ++ extraExtensions
    val extensionsCheck: F[ValidationReport] =
      if (required.isEmpty) ValidationReport.empty.pure[F]
      else
        session.execute(extensionsQuery).map { installed =>
          val missing = (required -- installed.toSet).toList.sorted
          ValidationReport(missing.map(Mismatch.ExtensionMissing(_)))
        }
    for {
      ext    <- extensionsCheck
      perRel <- relations.toList.traverse(validateOne(session, _))
      relRep = ValidationReport(perRel.flatMap(_.mismatches))
    } yield ext ++ relRep
  }

  /** Varargs convenience: report-only with no extra extensions. */
  def validate[F[_]: Concurrent](session: Session[F], relations: Relation[?]*): F[ValidationReport] =
    validate(session, relations, Set.empty)

  /**
   * Fail-fast helper: raises [[SchemaValidationException]] if any mismatches are found.
   *
   * `extraExtensions` is forwarded to [[validate]] for function-only contribs (pgcrypto, fuzzystrmatch).
   */
  def validateOrRaise[F[_]: Concurrent](
    session: Session[F],
    relations: Seq[Relation[?]],
    extraExtensions: Set[String]
  ): F[Unit] =
    validate(session, relations, extraExtensions).flatMap { report =>
      if report.isValid then ().pure[F]
      else Concurrent[F].raiseError(new SchemaValidationException(report))
    }

  /** Varargs convenience: fail-fast with no extra extensions. */
  def validateOrRaise[F[_]: Concurrent](session: Session[F], relations: Relation[?]*): F[Unit] =
    validateOrRaise(session, relations, Set.empty)

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
            indexReport <- relation match {
              // Declaring an index opts the table in: a table that declares none isn't index-checked.
              case t: Table[?, ?] if t.indexes.nonEmpty =>
                session
                  .prepare(indexesQuery)
                  .flatMap(_.stream((schema, relation.name), 64).compile.toList)
                  .map(rows => diffIndexes(label, t.indexes, rows))
              case _ => ValidationReport.empty.pure[F]
            }
          } yield columnReport ++ constraintReport ++ indexReport
      }
    } yield report
  }

  /**
   * Compare declared non-unique indexes with the table's actual ones, by name. A declared index must exist with the
   * same normalised definition (method, key columns with their order modifiers, predicate); once a table declares
   * indexes, undeclared non-unique ones are reported too. PK / UNIQUE indexes are left to the constraint check.
   */
  private def diffIndexes(label: String, declared: List[IndexDef], rows: List[IndexRow]): ValidationReport = {
    val byName      = rows.map(r => r.name -> r).toMap
    val declSet     = declared.map(_.name).toSet
    val forDeclared = declared.flatMap { d =>
      byName.get(d.name) match {
        case None =>
          List(Mismatch.IndexMissing(label, d.name, d.definition))
        case Some(r) if normalise(r.definition) != normalise(d.definition) =>
          List(Mismatch.IndexDefinitionMismatch(label, d.name, d.definition, r.definition))
        case _ => Nil
      }
    }
    val extras =
      rows.filter(r => !r.unique && !r.primary && !declSet.contains(r.name))
        .map(r => Mismatch.ExtraIndex(label, r.name, r.definition))
    ValidationReport(forDeclared ++ extras)
  }

  /** Compare index definitions ignoring identifier quotes, parentheses, spacing and case (Postgres normalises them). */
  private def normalise(defn: String): String =
    defn.toLowerCase.replaceAll("[\"()\\s]", "")

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
            PgTypes.actualTypeName(
              info.dataType,
              info.udtName,
              info.charMaxLength,
              info.numericPrecision,
              info.numericScale,
              info.formattedType
            )
          val typeIssue =
            Option.when(expected != actual)(
              Mismatch.TypeMismatch(label, col.name, expected, actual)
            )
          val nullIssue =
            Option.when(!skipNullability && info.isNullable != col.isNullable)(
              Mismatch.NullabilityMismatch(label, col.name, col.isNullable, info.isNullable)
            )
          // Views can't declare `.withGenerated`, and their columns always report `NEVER`; only tables are checked.
          val generatedIssue =
            Option.when(!skipNullability && info.isGenerated != col.isGenerated)(
              Mismatch.GeneratedMismatch(label, col.name, col.isGenerated, info.isGenerated)
            )
          typeIssue.toList ++ nullIssue.toList ++ generatedIssue.toList
      }
    }

    val declaredNames   = declared.map(_.name).toSet
    val extraMismatches =
      actual.map(_.name).filterNot(declaredNames.contains).map(Mismatch.ExtraColumn(label, _))

    ValidationReport(declaredMismatches ++ extraMismatches)
  }

}
