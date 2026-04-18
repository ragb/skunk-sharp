package skunk.sharp

import skunk.Codec
import skunk.sharp.internal.CompileChecks
import skunk.sharp.pg.{PgTypeFor, PgTypes}

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
final class TableBuilder[Cols <: Tuple, Name <: String & Singleton](
  val name: Name,
  val schema: Option[String],
  val columns: Cols
) {

  /** Non-nullable column, explicit codec. */
  inline def column[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    primary: Boolean = false,
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, false]], Name] =
    appendCol(n, codec, isNullable = false, hasDefault = false, primary, unique)

  /**
   * Non-nullable column with inferred codec — summons `PgTypeFor[T]`. Use with a tag type for an unambiguous codec
   * pick: `.column[Varchar[256]]("email")`, `.column[Int2]("age")`, `.column[Numeric[10, 2]]("amount")`.
   *
   * Uses the continuation pattern so Scala can infer the column-name singleton from the first call-site argument after
   * the type argument is given explicitly.
   */
  inline def column[T](using pf: PgTypeFor[T]): TableBuilder.ColumnCont[T, Cols, Name, false, false] =
    new TableBuilder.ColumnCont[T, Cols, Name, false, false](this, pf.codec, isNullable = false, hasDefault = false)

  /** Non-nullable column with a database-side default, explicit codec. */
  inline def columnDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    primary: Boolean = false,
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, true]], Name] =
    appendCol(n, codec, isNullable = false, hasDefault = true, primary, unique)

  /** Non-nullable column with a database-side default, inferred codec. */
  inline def columnDefaulted[T](using pf: PgTypeFor[T]): TableBuilder.ColumnCont[T, Cols, Name, false, true] =
    new TableBuilder.ColumnCont[T, Cols, Name, false, true](this, pf.codec, isNullable = false, hasDefault = true)

  /**
   * Nullable column, explicit codec. Codec is wrapped with `.opt` internally — pass `Codec[T]`, not `Codec[Option[T]]`.
   */
  inline def columnOpt[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, false]], Name] =
    appendOptCol(n, codec, hasDefault = false, unique)

  /** Nullable column, inferred codec. The inferred codec is wrapped with `.opt` internally. */
  inline def columnOpt[T](using pf: PgTypeFor[T]): TableBuilder.OptColumnCont[T, Cols, Name, false] =
    new TableBuilder.OptColumnCont[T, Cols, Name, false](this, pf.codec, hasDefault = false)

  /** Nullable column with a database-side default, explicit codec. */
  inline def columnOptDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T],
    unique: Boolean = false
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, true]], Name] =
    appendOptCol(n, codec, hasDefault = true, unique)

  /** Nullable column with a database-side default, inferred codec. */
  inline def columnOptDefaulted[T](using pf: PgTypeFor[T]): TableBuilder.OptColumnCont[T, Cols, Name, true] =
    new TableBuilder.OptColumnCont[T, Cols, Name, true](this, pf.codec, hasDefault = true)

  /** Place the table in a non-default schema. */
  def inSchema(s: String): TableBuilder[Cols, Name] =
    new TableBuilder[Cols, Name](name, Some(s), columns)

  /** Finalise the builder. */
  def build: Table[Cols, Name] = Table[Cols, Name](name, schema, columns)

  private inline def appendCol[T, N <: String & Singleton, Null <: Boolean, Default <: Boolean](
    n: N,
    codec: Codec[T],
    isNullable: Null,
    hasDefault: Default,
    primary: Boolean,
    unique: Boolean
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, Null, Default]], Name] = {
    CompileChecks.requireColumnAbsent[Cols, N]
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
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, Default]], Name] = {
    CompileChecks.requireColumnAbsent[Cols, N]
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

object TableBuilder {

  /**
   * Continuation returned by the inferred-codec `column[T]` / `columnDefaulted[T]` entry points. Carries the codec
   * already resolved via `PgTypeFor[T]`; the `.apply` call then accepts the column name (as a singleton literal) and
   * optional `primary` / `unique` flags. Splitting into two calls lets Scala infer `N` after `T` has been given
   * explicitly — Scala 3's all-or-nothing type-parameter inference makes the direct single-call form awkward to
   * overload.
   */
  final class ColumnCont[T, Cols <: Tuple, Name <: String & Singleton, Null <: Boolean, Default <: Boolean](
    b: TableBuilder[Cols, Name],
    codec: Codec[T],
    isNullable: Null,
    hasDefault: Default
  ) {

    inline def apply[N <: String & Singleton](
      n: N,
      primary: Boolean = false,
      unique: Boolean = false
    ): TableBuilder[Tuple.Append[Cols, Column[T, N, Null, Default]], Name] = {
      CompileChecks.requireColumnAbsent[Cols, N]
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
        b.name,
        b.schema,
        (b.columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, Null, Default]]]
      )
    }

  }

  /** Continuation for the nullable inferred-codec entry points. Wraps the codec in `.opt` at append time. */
  final class OptColumnCont[T, Cols <: Tuple, Name <: String & Singleton, Default <: Boolean](
    b: TableBuilder[Cols, Name],
    codec: Codec[T],
    hasDefault: Default
  ) {

    inline def apply[N <: String & Singleton](
      n: N,
      unique: Boolean = false
    ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, Default]], Name] = {
      CompileChecks.requireColumnAbsent[Cols, N]
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
        b.name,
        b.schema,
        (b.columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, Default]]]
      )
    }

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
 * shape when the caller does not ask for something else (`.to[T]`, an explicit column projection, etc.).
 */
type NamedRowOf[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], ValuesOf[Cols]]
