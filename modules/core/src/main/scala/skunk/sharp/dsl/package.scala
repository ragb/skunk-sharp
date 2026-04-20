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

/**
 * Single-import DSL entry point.
 *
 * {{{
 *   import skunk.sharp.dsl.*
 * }}}
 *
 * brings into scope everything needed to use the library:
 *
 *   - Verb entry points: `select`, `insert`, `update`, `delete` (defined in `Select.scala`, `Insert.scala`,
 *     `Update.scala`, `Delete.scala`).
 *   - Structural types: `Table`, `View`, `Column`, `TypedExpr`, `TypedColumn`, `ColumnsView`, `Relation`, `NamedRowOf`,
 *     built-in `Pg` functions, `PgFunction` / `PgOperator` builders.
 *   - WHERE operators and predicate combinators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `in`, `like`, `ilike`, `isNull`,
 *     `isNotNull`, `&&`, `||`, unary `!`), plus `ORDER BY` suffixes (`.asc`, `.desc`) and UPDATE `:=`.
 *
 * Nothing else ought to be imported to write a query.
 */
package object dsl {

  // ---- Types ----
  type Table[Cols <: Tuple, Name <: String & Singleton] = skunk.sharp.Table[Cols, Name]
  val Table: skunk.sharp.Table.type = skunk.sharp.Table

  type View[Cols <: Tuple, Name <: String & Singleton] = skunk.sharp.View[Cols, Name]
  val View: skunk.sharp.View.type = skunk.sharp.View

  type Relation[Cols <: Tuple] = skunk.sharp.Relation[Cols]

  /** Dedicated empty relation — `empty.select(_ => Pg.now)` renders as `SELECT now()`. */
  val empty: skunk.sharp.empty.type = skunk.sharp.empty

  type Column[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple] =
    skunk.sharp.Column[T, N, Null, Attrs]

  val ColumnAttr: skunk.sharp.ColumnAttr.type = skunk.sharp.ColumnAttr
  type ColumnAttr = skunk.sharp.ColumnAttr

  type TypedExpr[T] = skunk.sharp.TypedExpr[T]
  val TypedExpr: skunk.sharp.TypedExpr.type = skunk.sharp.TypedExpr

  type AliasedExpr[T, N <: String & Singleton] = skunk.sharp.AliasedExpr[T, N]

  // Extension methods on TypedExpr[T] (cast, as). Exported here so callers that only pull `skunk.sharp.dsl.*` still
  // see them.
  export skunk.sharp.{as, cast}

  type TypedColumn[T, Null <: Boolean, N <: String & Singleton] = skunk.sharp.TypedColumn[T, Null, N]

  type ColumnsView[Cols <: Tuple] = skunk.sharp.ColumnsView[Cols]
  val ColumnsView: skunk.sharp.ColumnsView.type = skunk.sharp.ColumnsView

  type NamedRowOf[Cols <: Tuple]                         = skunk.sharp.NamedRowOf[Cols]
  type NamesOf[Cols <: Tuple]                            = skunk.sharp.NamesOf[Cols]
  type ValuesOf[Cols <: Tuple]                           = skunk.sharp.ValuesOf[Cols]
  type HasColumn[Cols <: Tuple, N <: String & Singleton] = skunk.sharp.HasColumn[Cols, N]

  // ---- Postgres functions / operators ----
  val PgFunction: skunk.sharp.PgFunction.type = skunk.sharp.PgFunction
  val PgOperator: skunk.sharp.PgOperator.type = skunk.sharp.PgOperator
  val Pg: skunk.sharp.Pg.type                 = skunk.sharp.Pg

  // ---- WHERE predicate type and combinators ----
  type Where = skunk.sharp.where.Where
  val Where: skunk.sharp.where.Where.type = skunk.sharp.where.Where

  export skunk.sharp.where.{!==, &&, <, <=, ===, ====, >, >=, ||, and, ilike, in, isNotNull, isNull, like, not, or}
  export skunk.sharp.where.Stripped

  // ---- Schema validation ----
  val SchemaValidator: skunk.sharp.validation.SchemaValidator.type = skunk.sharp.validation.SchemaValidator
  type ValidationReport = skunk.sharp.validation.ValidationReport

  val ValidationReport: skunk.sharp.validation.ValidationReport.type =
    skunk.sharp.validation.ValidationReport

  type Mismatch = skunk.sharp.validation.Mismatch
  val Mismatch: skunk.sharp.validation.Mismatch.type = skunk.sharp.validation.Mismatch
  type SchemaValidationException = skunk.sharp.validation.SchemaValidationException

  // ---- Postgres types ----
  // We reuse skunk's [[skunk.data.Type]] directly. `PgTypes` carries the information_schema.data_type mapping used
  // by the schema validator and the SQL cast-name helper used by [[TypedExpr.cast]].
  val PgTypes: skunk.sharp.pg.PgTypes.type = skunk.sharp.pg.PgTypes
  type PgTypeFor[T] = skunk.sharp.pg.PgTypeFor[T]
  val PgTypeFor: skunk.sharp.pg.PgTypeFor.type = skunk.sharp.pg.PgTypeFor

  // ---- Arrays ----
  // `Arr[T]` is skunk's native Postgres-array type; `skunk.sharp.pg.arrays.given` ships `PgTypeFor[Arr[T]]` for
  // primitive element types plus a generic cats `Alternative + Foldable` derivation that covers `List`, `Vector`,
  // `Chain`, `LazyList`, …. `.to[F]` / `.toArr` bridge between skunk's `Arr[T]` and any cats-foldable collection.
  //
  // Array operators (`@>`, `<@`, `&&`, `||`, `= ANY(…)` as `.contains` / `.containedBy` / `.overlaps` / `.concat` /
  // `.elemOf`) and functions live in [[skunk.sharp.pg.ArrayOps]] / [[skunk.sharp.Pg]] — `.contains` / `.containedBy`
  // are intentionally *not* re-exported here because they clash with similarly-named extensions in the circe module
  // (jsonb `@>` / `<@`). Import `skunk.sharp.pg.ArrayOps.*` where array ops are needed.
  type Arr[T] = skunk.data.Arr[T]
  val Arr: skunk.data.Arr.type = skunk.data.Arr
  export skunk.sharp.pg.arrays.given
  export skunk.sharp.pg.arrays.{to, toArr}

  // ---- Literal shorthands ----
  //
  // `lit(1)` / `lit("x")` is the short form of [[skunk.sharp.TypedExpr.lit]]. Anywhere a `TypedExpr[T]` is required
  // (SELECT projection, UPDATE SET RHS, function arguments, …), `lit(v)` lifts a Scala value into a bound-parameter
  // expression. Deliberately not an implicit conversion — Scala 3 discourages those and the extra two characters
  // keep the lifting point explicit for readers.
  //
  // WHERE operators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `in`, `like`, `ilike`) already accept raw values directly
  // on the RHS, so `.where(u => u.age >= 18)` works without `lit`.
  inline def lit[T](v: T)(using pf: PgTypeFor[T]): skunk.sharp.TypedExpr[T] = skunk.sharp.TypedExpr.lit(v)

}
