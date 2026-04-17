/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package skunk.sharp

/** Type-level predicate: reduces to `true` iff `Cols` contains a [[Column]] whose singleton-type name equals `N`.
  *
  * Used by constraint modifiers (`withPrimary`, `withDefault`, `withUnique`) to refuse column names at compile time that
  * aren't part of the table's declared shape.
  */
type HasColumn[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match
  case Column[t, N, n, d] *: tail => true
  case h *: tail                  => HasColumn[tail, N]
  case EmptyTuple                 => false

/** Type-level lookup: resolves to the [[Column]] in `Cols` whose singleton-type name equals `N`. Does not reduce when
  * `N` is absent — that "stuck" match type produces a compile error at the use site.
  */
type ColumnAt[Cols <: Tuple, N <: String & Singleton] = Cols match
  case Column[t, N, n, d] *: tail => Column[t, N, n, d]
  case h *: tail                  => ColumnAt[tail, N]

/** Type-level extraction: the Scala value type of the column named `N` in `Cols`. */
type ColumnType[Cols <: Tuple, N <: String & Singleton] = Cols match
  case Column[t, N, n, d] *: tail => t
  case h *: tail                  => ColumnType[tail, N]

/** Type-level extraction: the nullability flag of the column named `N` in `Cols`. */
type ColumnNullable[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match
  case Column[t, N, n, d] *: tail => n
  case h *: tail                  => ColumnNullable[tail, N]
