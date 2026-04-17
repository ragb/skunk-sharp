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

/** Description of a Postgres table.
  *
  *   - `Row` — the user-facing row type; may be a case class, a tuple, or any type the builder can map to/from the
  *     underlying column tuple.
  *   - `Cols` — a heterogeneous tuple of [[Column]]s describing the table's shape. This is the authoritative source of
  *     truth for column names, types, nullability, and defaults, and is what the DSL's match types consume.
  */
final case class Table[Row, Cols <: Tuple](
  name: String,
  schema: Option[String],
  columns: Cols
):

  def qualifiedName: String =
    schema.fold(quoteIdent(name))(s => s"${quoteIdent(s)}.${quoteIdent(name)}")

  private def quoteIdent(s: String): String = s""""$s""""

object Table:
  // Builder construction (column-by-column path) and Table.of[T] macro are defined in their own files.
