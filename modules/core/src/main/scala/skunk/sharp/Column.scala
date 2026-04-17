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

import skunk.Codec
import skunk.sharp.pg.PgType

/** Phantom-typed column descriptor.
  *
  *   - `T` — the Scala value type of the column (use `Option[_]` for nullable columns).
  *   - `N` — a **singleton** string type carrying the column's name. Keeping the name in the type lets the DSL's match
  *     types (`HasColumn`, `ColumnAt`) look up columns by name at compile time.
  *   - `Null` — `true` iff the column is nullable.
  *   - `Default` — `true` iff the column has a database-side default (and therefore may be omitted in INSERT).
  */
final case class Column[T, N <: String & Singleton, Null <: Boolean, Default <: Boolean](
  name: N,
  pgType: PgType,
  codec: Codec[T],
  isNullable: Null,
  hasDefault: Default,
  isPrimary: Boolean = false,
  isUnique: Boolean = false
):

  def qualifiedIdent: String = s""""$name""""

  def markPrimary: Column[T, N, Null, Default] = copy(isPrimary = true)
  def markUnique: Column[T, N, Null, Default]  = copy(isUnique = true)
