package skunk.sharp

import skunk.Codec
import skunk.sharp.internal.CompileChecks
import skunk.sharp.pg.{PgTypeFor, PgTypes}

/**
 * Column-by-column [[Table]] builder — **the recommended way to describe a table**.
 *
 * Each column is declared with its exact skunk [[skunk.Codec]] (from `skunk.codec.all`); the column's Postgres type is
 * derived from the codec via `codec.types.head`. You never have to agree with the library about whether `String` maps
 * to `text` or `varchar(n)`. Nullability is expressed by calling `.columnOpt`; defaults are expressed by
 * `.columnDefaulted` or `.columnOptDefaulted`. Primary-key / unique constraints are declared post-`build` via
 * `.withPrimary(n)` / `.withUnique(n)` — those methods append `ColumnAttr.Primary` / `ColumnAttr.Unique` markers so
 * `.onConflict(c => c.<n>)` is accepted at compile time.
 *
 * {{{
 *   import skunk.codec.all.*    // uuid, varchar, int4, timestamptz, … — skunk's codecs
 *
 *   val users = Table.builder("users")
 *     .column("id",          uuid)
 *     .column("email",       varchar(256))
 *     .column("age",         int4)
 *     .columnDefaulted("created_at", timestamptz)
 *     .columnOpt("deleted_at",       timestamptz)
 *     .build
 *     .withPrimary("id")
 *     .withUnique("email")
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
    codec: Codec[T]
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, EmptyTuple]], Name] =
    appendCol[T, N, false, EmptyTuple](n, codec, isNullable = false, hasDefault = false)

  /**
   * Non-nullable column with inferred codec — summons `PgTypeFor[T]`. Use with a tag type for an unambiguous codec
   * pick: `.column[Varchar[256]]("email")`, `.column[Int2]("age")`, `.column[Numeric[10, 2]]("amount")`.
   *
   * Uses the continuation pattern so Scala can infer the column-name singleton from the first call-site argument after
   * the type argument is given explicitly.
   */
  inline def column[T](using pf: PgTypeFor[T]): TableBuilder.ColumnCont[T, Cols, Name, false, EmptyTuple] =
    new TableBuilder.ColumnCont[T, Cols, Name, false, EmptyTuple](
      this,
      pf.codec,
      isNullable = false,
      hasDefault = false
    )

  /** Non-nullable column with a database-side default, explicit codec. */
  inline def columnDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, false, ColumnAttr.Default *: EmptyTuple]], Name] =
    appendCol[T, N, false, ColumnAttr.Default *: EmptyTuple](n, codec, isNullable = false, hasDefault = true)

  /** Non-nullable column with a database-side default, inferred codec. */
  inline def columnDefaulted[T](using
    pf: PgTypeFor[T]
  ): TableBuilder.ColumnCont[T, Cols, Name, false, ColumnAttr.Default *: EmptyTuple] =
    new TableBuilder.ColumnCont[T, Cols, Name, false, ColumnAttr.Default *: EmptyTuple](
      this,
      pf.codec,
      isNullable = false,
      hasDefault = true
    )

  /**
   * Nullable column, explicit codec. Codec is wrapped with `.opt` internally — pass `Codec[T]`, not `Codec[Option[T]]`.
   */
  inline def columnOpt[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, EmptyTuple]], Name] =
    appendOptCol[T, N, EmptyTuple](n, codec, hasDefault = false)

  /** Nullable column, inferred codec. The inferred codec is wrapped with `.opt` internally. */
  inline def columnOpt[T](using pf: PgTypeFor[T]): TableBuilder.OptColumnCont[T, Cols, Name, EmptyTuple] =
    new TableBuilder.OptColumnCont[T, Cols, Name, EmptyTuple](this, pf.codec, hasDefault = false)

  /** Nullable column with a database-side default, explicit codec. */
  inline def columnOptDefaulted[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, ColumnAttr.Default *: EmptyTuple]], Name] =
    appendOptCol[T, N, ColumnAttr.Default *: EmptyTuple](n, codec, hasDefault = true)

  /** Nullable column with a database-side default, inferred codec. */
  inline def columnOptDefaulted[T](using
    pf: PgTypeFor[T]
  ): TableBuilder.OptColumnCont[T, Cols, Name, ColumnAttr.Default *: EmptyTuple] =
    new TableBuilder.OptColumnCont[T, Cols, Name, ColumnAttr.Default *: EmptyTuple](
      this,
      pf.codec,
      hasDefault = true
    )

  /** Place the table in a non-default schema. */
  def inSchema(s: String): TableBuilder[Cols, Name] =
    new TableBuilder[Cols, Name](name, Some(s), columns)

  /** Finalise the builder. */
  def build: Table[Cols, Name] = Table[Cols, Name](name, schema, columns)

  private inline def appendCol[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    n: N,
    codec: Codec[T],
    isNullable: Null,
    hasDefault: Boolean
  ): TableBuilder[Tuple.Append[Cols, Column[T, N, Null, Attrs]], Name] = {
    CompileChecks.requireColumnAbsent[Cols, N]
    val col = Column[T, N, Null, Attrs](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec,
      isNullable = isNullable,
      hasDefault = hasDefault,
      isPrimary = false,
      isUnique = false
    )
    new TableBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, Null, Attrs]]]
    )
  }

  private inline def appendOptCol[T, N <: String & Singleton, Attrs <: Tuple](
    n: N,
    codec: Codec[T],
    hasDefault: Boolean
  ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, Attrs]], Name] = {
    CompileChecks.requireColumnAbsent[Cols, N]
    val col = Column[Option[T], N, true, Attrs](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec.opt,
      isNullable = true,
      hasDefault = hasDefault,
      isPrimary = false,
      isUnique = false
    )
    new TableBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, Attrs]]]
    )
  }

}

object TableBuilder {

  /**
   * Continuation returned by the inferred-codec `column[T]` / `columnDefaulted[T]` entry points. Carries the codec
   * already resolved via `PgTypeFor[T]`; the `.apply` call then accepts the column name (as a singleton literal).
   * Splitting into two calls lets Scala infer `N` after `T` has been given explicitly — Scala 3's all-or-nothing
   * type-parameter inference makes the direct single-call form awkward to overload.
   */
  final class ColumnCont[T, Cols <: Tuple, Name <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    b: TableBuilder[Cols, Name],
    codec: Codec[T],
    isNullable: Null,
    hasDefault: Boolean
  ) {

    inline def apply[N <: String & Singleton](
      n: N
    ): TableBuilder[Tuple.Append[Cols, Column[T, N, Null, Attrs]], Name] = {
      CompileChecks.requireColumnAbsent[Cols, N]
      val col = Column[T, N, Null, Attrs](
        name = n,
        tpe = PgTypes.typeOf(codec),
        codec = codec,
        isNullable = isNullable,
        hasDefault = hasDefault,
        isPrimary = false,
        isUnique = false
      )
      new TableBuilder(
        b.name,
        b.schema,
        (b.columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, Null, Attrs]]]
      )
    }

  }

  /** Continuation for the nullable inferred-codec entry points. Wraps the codec in `.opt` at append time. */
  final class OptColumnCont[T, Cols <: Tuple, Name <: String & Singleton, Attrs <: Tuple](
    b: TableBuilder[Cols, Name],
    codec: Codec[T],
    hasDefault: Boolean
  ) {

    inline def apply[N <: String & Singleton](
      n: N
    ): TableBuilder[Tuple.Append[Cols, Column[Option[T], N, true, Attrs]], Name] = {
      CompileChecks.requireColumnAbsent[Cols, N]
      val col = Column[Option[T], N, true, Attrs](
        name = n,
        tpe = PgTypes.typeOf(codec),
        codec = codec.opt,
        isNullable = true,
        hasDefault = hasDefault,
        isPrimary = false,
        isUnique = false
      )
      new TableBuilder(
        b.name,
        b.schema,
        (b.columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, Attrs]]]
      )
    }

  }

}

/** Type-level extraction: the tuple of column *names* (singleton strings) declared by `Cols`. */
type NamesOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, attrs] *: tail => n *: NamesOf[tail]
  case EmptyTuple                      => EmptyTuple
}

/**
 * Type-level extraction: the tuple of column *value types* declared by `Cols`. Nullable columns contribute `Option[T]`.
 */
type ValuesOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, attrs] *: tail => t *: ValuesOf[tail]
  case EmptyTuple                      => EmptyTuple
}

/**
 * Default whole-row named tuple built from `Cols`. Query and insert builders will use it as the natural projection
 * shape when the caller does not ask for something else (`.to[T]`, an explicit column projection, etc.).
 */
type NamedRowOf[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], ValuesOf[Cols]]
