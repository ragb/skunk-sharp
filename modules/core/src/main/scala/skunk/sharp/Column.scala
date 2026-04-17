package skunk.sharp

import skunk.Codec
import skunk.data.Type

/**
 * Phantom-typed column descriptor.
 *
 *   - `T` — the Scala value type of the column (use `Option[_]` for nullable columns).
 *   - `N` — a **singleton** string type carrying the column's name. Keeping the name in the type lets the DSL's match
 *     types (`HasColumn`, `ColumnAt`) look up columns by name at compile time.
 *   - `Null` — `true` iff the column is nullable.
 *   - `Default` — `true` iff the column has a database-side default (and therefore may be omitted in INSERT).
 *
 * `tpe` is the skunk [[skunk.data.Type]] of the column — we store it rather than a custom enum so we inherit skunk's
 * full built-in type registry.
 */
final case class Column[T, N <: String & Singleton, Null <: Boolean, Default <: Boolean](
  name: N,
  tpe: Type,
  codec: Codec[T],
  isNullable: Null,
  hasDefault: Default,
  isPrimary: Boolean = false,
  isUnique: Boolean = false
) {

  def qualifiedIdent: String = s""""$name""""
}
