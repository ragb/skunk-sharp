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

  /** The column is (part of) a declared primary key. */
  sealed trait Primary extends ColumnAttr

  /** The column is backed by a single-column `UNIQUE` constraint. */
  sealed trait Unique extends ColumnAttr

}

/**
 * Phantom-typed column descriptor.
 *
 *   - `T` — the Scala value type of the column (use `Option[_]` for nullable columns).
 *   - `N` — a **singleton** string type carrying the column's name. Keeping the name in the type lets the DSL's match
 *     types (`HasColumn`, `ColumnAt`) look up columns by name at compile time.
 *   - `Null` — `true` iff the column is nullable.
 *   - `Attrs` — a tuple of [[ColumnAttr]] markers: `Default`, `Primary`, `Unique` (and any third-party additions).
 *     Checked by [[Contains]] / [[HasUniqueness]] / [[ColumnDefault]] to offer compile-time evidence for insert
 *     defaulting and `.onConflict(...)` targeting.
 *
 * Term-level `hasDefault` / `isPrimary` / `isUnique` flags mirror the phantom markers and are what the schema
 * validator reads at runtime when diffing against `information_schema.table_constraints`.
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
  isUnique: Boolean = false
) {

  def qualifiedIdent: String = s""""$name""""
}
