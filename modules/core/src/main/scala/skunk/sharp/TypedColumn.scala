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

/**
 * A typed reference to a column in the current WHERE/SELECT lambda.
 *
 *   - `T` — the Scala value type.
 *   - `Null` — nullability tracked at the type level so operators like `isNull` can be offered only on nullable cols.
 *   - `N` — the column's name as a singleton string. Preserved so `.onConflict(c => c.id)` can carry the target name
 *     into compile-time evidence (`HasUniqueness[Cols, N]`).
 *
 * `TypedColumn` is a [[TypedExpr]] leaf — operators produce new `TypedExpr`s.
 */
final class TypedColumn[T, Null <: Boolean, N <: String & Singleton](
  val name: N,
  val codec: Codec[T],
  val qualifier: Option[String] = None,
  val quoteQualifier: Boolean = true
) extends TypedExpr[T] {

  /** SQL identifier form — `"name"` or `"alias"."name"` / `alias."name"` depending on qualifier state. */
  def sqlRef: String =
    qualifier match {
      case None                      => s""""$name""""
      case Some(q) if quoteQualifier => s""""$q"."$name""""
      case Some(q) /* unquoted */    => s"""$q."$name""""
    }

  def render: skunk.AppliedFragment = TypedExpr.raw(sqlRef)

}

object TypedColumn {

  /**
   * Build a `TypedColumn` from a [[Column]] descriptor. The column-name singleton type is preserved on the resulting
   * `TypedColumn` so downstream DSL features (notably `.onConflict`'s `HasUniqueness` evidence) can read it.
   */
  def of[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs]
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec)

  /**
   * Build a `TypedColumn` whose rendering is prefixed with a table/alias qualifier (`"u"."col"`). By default the
   * qualifier is double-quoted — safe for arbitrary user-provided aliases.
   */
  def qualified[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs],
    qualifier: String
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec, Some(qualifier), quoteQualifier = true)

  /**
   * Same as [[qualified]] but leaves the qualifier unquoted in the SQL (`excluded."col"` instead of
   * `"excluded"."col"`). Needed for Postgres pseudo-tables like `excluded` — a quoted `"excluded"` would be treated as
   * a user identifier and fail to reference the incoming-row pseudo-table in `ON CONFLICT DO UPDATE`.
   */
  def qualifiedRaw[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs],
    qualifier: String
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec, Some(qualifier), quoteQualifier = false)

}
