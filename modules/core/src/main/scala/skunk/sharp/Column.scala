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

package skunk.sharp

import skunk.Codec
import skunk.data.Type

/**
 * Type-level attribute markers carried on [[Column]]'s `Attrs` phantom tuple. Each marker is an empty sealed trait used
 * only as a phantom — never instantiated. A column's `Attrs` tuple contains zero or more of these, in any order;
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
 * Runtime mirror of [[ColumnAttr]] phantom markers — instantiated, carried in a column's `attrs` list, and consumed by
 * the schema validator and any runtime code that needs to diff against `information_schema`. A single source of truth
 * replaces the old quartet of `isPrimary` / `isUnique` / `hasDefault` / `uniqueGroups` term fields.
 */
sealed trait ColumnAttrValue

object ColumnAttrValue {

  case object Default extends ColumnAttrValue

  /** Participation in the primary key — `members` is the full tuple of PK column names (a single-col PK has one). */
  final case class Pk(members: List[String]) extends ColumnAttrValue

  /** Participation in a named UNIQUE constraint — `members` lists every column the constraint covers. */
  final case class Uq(name: String, members: List[String]) extends ColumnAttrValue

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
 * `attrs` is the runtime mirror of the `Attrs` phantom — one list of [[ColumnAttrValue]]s carrying the same facts.
 * Schema validation and other runtime code reads this list directly; there's no separate set of boolean fields to keep
 * in sync.
 *
 * `tpe` is the skunk [[skunk.data.Type]] of the column — we store it rather than a custom enum so we inherit skunk's
 * full built-in type registry.
 */
final case class Column[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
  name: N,
  tpe: Type,
  codec: Codec[T],
  isNullable: Null,
  attrs: List[ColumnAttrValue] = Nil
) {

  def qualifiedIdent: String = s""""$name""""

  /** `true` iff the column participates in the primary key. Derived from [[attrs]]. */
  def isPrimary: Boolean = attrs.exists(_.isInstanceOf[ColumnAttrValue.Pk])

  /** `true` iff the column participates in any UNIQUE constraint. Derived from [[attrs]]. */
  def isUnique: Boolean = attrs.exists(_.isInstanceOf[ColumnAttrValue.Uq])

  /** `true` iff the column has a declared database-side default. Derived from [[attrs]]. */
  def hasDefault: Boolean = attrs.contains(ColumnAttrValue.Default)

  /** Names of the UNIQUE constraints this column participates in. Derived from [[attrs]]. */
  def uniqueGroups: Set[String] =
    attrs.iterator.collect { case ColumnAttrValue.Uq(name, _) => name }.toSet

}
