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

import skunk.AppliedFragment

/**
 * A named, column-typed Postgres relation — base class for [[Table]] (read/write), [[View]] (read-only), and any
 * joinable non-table source (a compiled select used as a derived table, future VALUES / set-returning functions, etc.).
 *
 * Everything that can be queried is a `Relation`: SELECT is defined once as an extension on `Relation`. Mutations
 * (INSERT, UPDATE, DELETE) live as extensions on [[Table]] alone, so attempting to mutate a [[View]] is a compile
 * error.
 */
trait Relation[Cols <: Tuple] {

  def name: String
  def schema: Option[String]
  def columns: Cols

  /**
   * Whether this relation is a base table (`"BASE TABLE"`) or a view (`"VIEW"`). Matches
   * `information_schema.tables.table_type`, so the schema validator can check the kind as well as the columns.
   *
   * Derived relations (subquery-as-relation, VALUES, set-returning functions, …) return `""` and aren't validated —
   * they're not registered in the catalogue.
   */
  def expectedTableType: String

  /**
   * `false` for the dedicated empty relation used to build FROM-less queries (`SELECT now()`); `true` for real tables
   * and views. Drives whether the SELECT compiler emits a `FROM …` clause.
   */
  def hasFromClause: Boolean = true

  def qualifiedName: String =
    schema.fold(quoteIdent(name))(s => s"${quoteIdent(s)}.${quoteIdent(name)}")

  /**
   * The SQL fragment rendered into a FROM / JOIN position when the relation is sourced with the given `alias`. For
   * tables / views this is typically `"schema"."name"` or `"name" AS "alias"`. Derived relations (subqueries, VALUES,
   * …) override to inline their own SQL — which may carry bound parameters, hence `AppliedFragment` rather than plain
   * `String`.
   *
   * Default implementation keeps the current table/view behaviour: emit `qualifiedName`, append `AS "<alias>"` only
   * when the alias differs from the relation's own name.
   */
  def fromFragment(alias: String): AppliedFragment = {
    val qn = qualifiedName
    if (alias == name) TypedExpr.raw(qn)
    else TypedExpr.raw(s"""$qn AS "$alias"""")
  }

  protected def quoteIdent(s: String): String = s""""$s""""
}

/**
 * The empty relation — no columns, no FROM clause. Use this to express queries that return only constants or
 * function-call results (`empty.select(_ => Pg.now)` → `SELECT now()`).
 *
 * Because its column tuple is `EmptyTuple`, `empty.select` with no explicit projection is useless (no columns to
 * project). The useful form is `empty.select(_ => <expr>)` or `empty.select(_ => (<e1>, <e2>))`.
 */
case object empty extends Relation[EmptyTuple] {
  val name: String                    = ""
  val schema: Option[String]          = None
  val columns: EmptyTuple             = EmptyTuple
  val expectedTableType: String       = ""
  override val hasFromClause: Boolean = false
  override val qualifiedName: String  = ""
}
