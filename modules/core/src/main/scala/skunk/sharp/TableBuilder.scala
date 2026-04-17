package skunk.sharp

import skunk.Codec
import skunk.sharp.pg.PgTypes

/**
 * Column-by-column [[Table]] builder — **the recommended way to describe a table**.
 *
 * Each column is declared with its exact skunk [[skunk.Codec]] (from `skunk.codec.all`); the column's Postgres type is
 * derived from the codec via `codec.types.head`. You never have to agree with the library about whether `String` maps
 * to `text` or `varchar(n)`. Nullability is expressed by calling `.columnOpt` (the codec is wrapped in `.opt`
 * internally); defaults are expressed by `.columnDefaulted` or `.columnOptDefaulted`.
 *
 * {{{
 *   import skunk.codec.all.*    // uuid, varchar, int4, timestamptz, … — skunk's codecs
 *
 *   val users = Table.builder("users")
 *     .column("id",          uuid,           primary = true)
 *     .column("email",       varchar(256),   unique  = true)
 *     .column("age",         int4)
 *     .columnDefaulted("created_at", timestamptz)
 *     .columnOpt("deleted_at",       timestamptz)
 *     .build
 * }}}
 */
final class TableBuilder[Cols <: Tuple](
  val name: String,
  val schema: Option[String],
  val columns: Cols
) {

  /** Non-nullable column. */
  inline def column[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    primary: Boolean = false,
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, false]]] =
    appendCol(n, codec, isNullable = false, hasDefault = false, primary, unique)

  /** Non-nullable column with a database-side default. */
  inline def columnDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    primary: Boolean = false,
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, true]]] =
    appendCol(n, codec, isNullable = false, hasDefault = true, primary, unique)

  /** Nullable column. The codec is wrapped with `.opt` internally, so pass a `Codec[T]` (not `Codec[Option[T]]`). */
  inline def columnOpt[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, false]]] =
    appendOptCol(n, codec, hasDefault = false, unique)

  /** Nullable column with a database-side default. */
  inline def columnOptDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, true]]] =
    appendOptCol(n, codec, hasDefault = true, unique)

  /** Place the table in a non-default schema. */
  def inSchema(s: String): TableBuilder[Cols] =
    new TableBuilder[Cols](name, Some(s), columns)

  /** Finalise the builder. */
  def build: Table[Cols] = Table[Cols](name, schema, columns)

  private inline def appendCol[T, N <: String & Singleton, Null <: Boolean, Default <: Boolean](
    n: N,
    codec: Codec[T],
    isNullable: Null,
    hasDefault: Default,
    primary: Boolean,
    unique: Boolean
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, Null, Default]]] = {
    val col = Column[T, N, Null, Default](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec,
      isNullable = isNullable,
      hasDefault = hasDefault,
      isPrimary = primary,
      isUnique = unique
    )
    new TableBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, Null, Default]]]
    )
  }

  private inline def appendOptCol[T, N <: String & Singleton, Default <: Boolean](
    n: N,
    codec: Codec[T],
    hasDefault: Default,
    unique: Boolean
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, Default]]] = {
    val col = Column[Option[T], N, true, Default](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec.opt,
      isNullable = true,
      hasDefault = hasDefault,
      isPrimary = false,
      isUnique = unique
    )
    new TableBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, Default]]]
    )
  }

}

/** Type-level extraction: the tuple of column *names* (singleton strings) declared by `Cols`. */
type NamesOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, d] *: tail => n *: NamesOf[tail]
  case EmptyTuple                  => EmptyTuple
}

/**
 * Type-level extraction: the tuple of column *value types* declared by `Cols`. Nullable columns contribute `Option[T]`.
 */
type ValuesOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, d] *: tail => t *: ValuesOf[tail]
  case EmptyTuple                  => EmptyTuple
}

/**
 * Default whole-row named tuple built from `Cols`. Query and insert builders will use it as the natural projection
 * shape when the caller does not ask for something else (`.as[T]`, an explicit column projection, etc.).
 */
type NamedRowOf[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], ValuesOf[Cols]]
