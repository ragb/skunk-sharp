package skunk.sharp

import skunk.Codec
import skunk.data.Type

/**
 * Type-level attribute markers carried on [[Column]]'s `Attrs` phantom tuple. Each marker is an empty sealed trait
 * used only as a phantom — never instantiated. A column's `Attrs` tuple contains zero or more of these, in any order;
 * [[Contains]] is the only operation that inspects them.
 *
 * Third-party modules can ship their own markers (e.g. `Generated`, `Identity`) by defining a trait and an
 * `AddAttr`-style type function; the core doesn't need to know about them.
 */
sealed trait ColumnAttr

object ColumnAttr {

  /** The column has a database-side default (sequence PK, `DEFAULT now()`, …) — may be omitted from INSERT. */
  sealed trait Default extends ColumnAttr

  /**
   * Primary-key membership. `Members` is the tuple of all column-name singletons that make up the primary key; every
   * column that is part of the PK carries the *same* `Pk[Members]` value. Single-column PKs use a one-element tuple
   * (`Pk[("id")]`); composite PKs share the full member tuple (`Pk[("tenant_id", "event_id")]`).
   *
   * Using a shared members tuple lets composite `.onConflict(c => (c.a, c.b))` verify that the lambda's column set
   * matches the declared PK exactly.
   */
  sealed trait Pk[Members <: Tuple] extends ColumnAttr

  /**
   * `UNIQUE` constraint membership. `Name` is the constraint's declared name (used for schema-validator diffs and for
   * distinguishing multiple unique constraints on the same column). `Members` is the tuple of column names covered by
   * this constraint; every column in the group carries the *same* `Uq[Name, Members]`.
   *
   * Single-column `.withUnique("email")` uses `Uq["email", ("email")]` (auto-names after the column). Composite
   * `.withUniqueIndex("uq_tenant_slug", "tenant_id", "slug")` uses `Uq["uq_tenant_slug", ("tenant_id", "slug")]`.
   */
  sealed trait Uq[Name <: String & Singleton, Members <: Tuple] extends ColumnAttr

}

/**
 * Phantom-typed column descriptor.
 *
 *   - `T` — the Scala value type of the column (use `Option[_]` for nullable columns).
 *   - `N` — a **singleton** string type carrying the column's name. Keeping the name in the type lets the DSL's match
 *     types (`HasColumn`, `ColumnAt`) look up columns by name at compile time.
 *   - `Null` — `true` iff the column is nullable.
 *   - `Attrs` — a tuple of [[ColumnAttr]] markers: `Default`, `Pk[Members]`, `Uq[Name, Members]` (and any third-party
 *     additions). Checked by [[Contains]] / [[HasUniqueness]] / [[HasCompositeUniqueness]] / [[ColumnDefault]] for
 *     compile-time evidence of insert defaulting and `.onConflict(...)` targeting.
 *
 * Term-level `hasDefault` / `isPrimary` / `isUnique` flags mirror the phantom markers for simple single-column checks;
 * the table-level [[Table.pk]] / [[Table.uniques]] lists carry the full composite constraint shape for the schema
 * validator to diff against `information_schema.table_constraints`.
 *
 * `tpe` is the skunk [[skunk.data.Type]] of the column — we store it rather than a custom enum so we inherit skunk's
 * full built-in type registry.
 */
final case class Column[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
  name: N,
  tpe: Type,
  codec: Codec[T],
  isNullable: Null,
  hasDefault: Boolean = false,
  isPrimary: Boolean = false,
  isUnique: Boolean = false,
  uniqueGroups: Set[String] = Set.empty
) {

  def qualifiedIdent: String = s""""$name""""
}
