package skunk.sharp

import skunk.sharp.internal.{deriveColumns, ColumnsFromMirror, CompileChecks}

import scala.deriving.Mirror

/**
 * A writable Postgres relation (a `BASE TABLE`).
 *
 * `Cols` is a heterogeneous tuple of [[Column]]s describing the table's shape. It is the authoritative source of truth
 * for column names, types, nullability, and defaults; the DSL's match types consume it for compile-time query checking.
 *
 * `Name` is the singleton type of the table's name — carried so JOIN extensions can default the alias to the table's
 * name when the user didn't supply an explicit `.alias(...)`.
 *
 * SELECT is available via the shared [[Relation]] extension. INSERT / UPDATE / DELETE are extensions on `Table`
 * specifically — attempting them on a [[View]] is a compile error.
 */
final case class Table[Cols <: Tuple, Name <: String & Singleton](
  name: Name,
  schema: Option[String],
  columns: Cols
) extends Relation[Cols] {

  val expectedTableType: String = "BASE TABLE"

  /** Place the table in a non-default schema. */
  def inSchema(s: String): Table[Cols, Name] = copy(schema = Some(s))

  /**
   * Primitive: rewrite one column's metadata with `f`. The lambda receives the column at its runtime erasure
   * (`Column[Any, N, Boolean, Boolean, Boolean, Boolean]`) and must return a column of the same shape — this is the
   * compromise that keeps the method usable while `Cols` is still abstract in the method body. In practice:
   *
   *   - `c.copy(tpe = …, codec = …)` — fine **as long as** the new codec's Scala value type matches the column's
   *     declared `T` (e.g. `Codec[String]` for a String column, not `Codec[Int]`). Use `.cast[U]` at query time for
   *     genuine value-type changes.
   *
   * Unknown names produce a friendly compile error. Use [[withPrimary]] / [[withUnique]] / [[withDefault]] for the
   * common constraint modifiers — those also flip the corresponding phantom type parameters on `Cols`.
   */
  inline def withColumn[N <: String & Singleton](inline n: N)(
    f: Column[Any, N, Boolean, Boolean, Boolean, Boolean] => Column[Any, N, Boolean, Boolean, Boolean, Boolean]
  ): Table[Cols, Name] = {
    CompileChecks.requireColumn[Cols, N]
    copy(columns = Table.updateCol[Cols, N](columns, n, f).asInstanceOf[Cols])
  }

  /**
   * Mark a column as primary key. Flips the `IsPrimary` phantom on that column so `.onConflict(c => c.<n>)` is
   * accepted at compile time.
   */
  inline def withPrimary[N <: String & Singleton](inline n: N): Table[Table.SetPrimary[Cols, N], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val updated = Table.updateCol[Cols, N](columns, n, _.copy(isPrimary = true))
    copy(columns = updated.asInstanceOf[Table.SetPrimary[Cols, N]])
      .asInstanceOf[Table[Table.SetPrimary[Cols, N], Name]]
  }

  /**
   * Mark a column as unique. Flips the `IsUnique` phantom on that column so `.onConflict(c => c.<n>)` is accepted at
   * compile time.
   */
  inline def withUnique[N <: String & Singleton](inline n: N): Table[Table.SetUnique[Cols, N], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val updated = Table.updateCol[Cols, N](columns, n, _.copy(isUnique = true))
    copy(columns = updated.asInstanceOf[Table.SetUnique[Cols, N]])
      .asInstanceOf[Table[Table.SetUnique[Cols, N], Name]]
  }

  /**
   * Override a column's skunk codec. The column's `tpe` (skunk `data.Type`) is derived from the codec, so pass
   * `varchar(256)` / `int8` / `uuid` (from `skunk.codec.all`) directly. The codec's value type `T` must match the
   * column's declared Scala value type (not statically checked because the match type doesn't reduce here; a mismatch
   * raises at the first row that encodes or decodes).
   */
  inline def withColumnCodec[N <: String & Singleton, T](inline n: N, codec: skunk.Codec[T]): Table[Cols, Name] =
    withColumn(n)(c =>
      c.copy(
        tpe = skunk.sharp.pg.PgTypes.typeOf(codec),
        codec = codec.asInstanceOf[skunk.Codec[Any]]
      )
    )

  /**
   * Mark a column as having a database-side default. Flips the `Default` phantom parameter on that column so INSERTs
   * that omit the column become legal at the type level.
   */
  inline def withDefault[N <: String & Singleton](inline n: N): Table[Table.SetDefault[Cols, N], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val updated = Table.updateCol[Cols, N](columns, n, _.copy(hasDefault = true))
    copy(columns = updated.asInstanceOf[Table.SetDefault[Cols, N]])
      .asInstanceOf[Table[Table.SetDefault[Cols, N], Name]]
  }

}

object Table {

  /** Entry point for the column-by-column builder path. */
  inline def builder[Name <: String & Singleton](name: Name): TableBuilder[EmptyTuple, Name] =
    new TableBuilder[EmptyTuple, Name](name, None, EmptyTuple)

  /**
   * Derive a [[Table]] from a case class via its `Mirror.ProductOf`. Only the case class's *shape* (field labels and
   * types) is used; the case class type itself is not remembered by `Table` — use `.to[User]` on a query to materialise
   * rows as `User` when you want to.
   *
   * Returns a continuation so the call site `Table.of[User]("users")` can specify `T` explicitly while `Name` is still
   * inferred from the name literal — Scala's all-or-nothing type-parameter inference forbids doing both in one call.
   */
  inline def of[T <: Product]: OfCont[T] = new OfCont[T]

  /** Continuation for [[of]]. Kept public because the builder / test code refers to it by path. */
  final class OfCont[T <: Product] {

    inline def apply[Name <: String & Singleton](tableName: Name)(using
      m: Mirror.ProductOf[T]
    ): Table[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes], Name] = {
      val cols = deriveColumns[m.MirroredElemLabels, m.MirroredElemTypes]
      Table[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes], Name](
        tableName,
        None,
        cols.asInstanceOf[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes]]
      )
    }

  }

  /** Type-level: flip the `Default` phantom on the column named `N`. */
  type SetDefault[Cols <: Tuple, N <: String & Singleton] <: Tuple = Cols match {
    case Column[t, N, nu, d, p, u] *: tail => Column[t, N, nu, true, p, u] *: tail
    case h *: tail                         => h *: SetDefault[tail, N]
    case EmptyTuple                        => EmptyTuple
  }

  /** Type-level: flip the `IsPrimary` phantom on the column named `N`. */
  type SetPrimary[Cols <: Tuple, N <: String & Singleton] <: Tuple = Cols match {
    case Column[t, N, nu, d, p, u] *: tail => Column[t, N, nu, d, true, u] *: tail
    case h *: tail                         => h *: SetPrimary[tail, N]
    case EmptyTuple                        => EmptyTuple
  }

  /** Type-level: flip the `IsUnique` phantom on the column named `N`. */
  type SetUnique[Cols <: Tuple, N <: String & Singleton] <: Tuple = Cols match {
    case Column[t, N, nu, d, p, u] *: tail => Column[t, N, nu, d, p, true] *: tail
    case h *: tail                         => h *: SetUnique[tail, N]
    case EmptyTuple                        => EmptyTuple
  }

  /** Runtime helper: walk the columns tuple, apply `f` to the column whose singleton-name matches `n`. */
  private[sharp] def updateCol[Cols <: Tuple, N <: String & Singleton](
    cols: Cols,
    n: N,
    f: Column[Any, N, Boolean, Boolean, Boolean, Boolean] => Column[Any, N, Boolean, Boolean, Boolean, Boolean]
  ): Tuple = {
    val updated = cols.toList.map {
      case c: Column[?, ?, ?, ?, ?, ?] if c.name == n =>
        f(c.asInstanceOf[Column[Any, N, Boolean, Boolean, Boolean, Boolean]])
      case other => other
    }
    Tuple.fromArray(updated.toArray[Any])
  }

}
