package skunk.sharp

import skunk.Codec
import skunk.sharp.internal.DeriveColumns
import skunk.sharp.pg.PgTypes

import scala.deriving.Mirror

/**
 * A read-only Postgres relation (a `VIEW` in `information_schema.tables`).
 *
 * Identical in shape to [[Table]], but the DSL deliberately withholds INSERT / UPDATE / DELETE — calling any of those
 * on a `View` is a compile error. SELECT is available (via the shared [[Relation]] extension).
 *
 * Because views are read-only, in principle we only need `Decoder[T]` per column; we accept `Codec[T]` for now so users
 * can pass skunk's ready-made codecs (which extend `Decoder`) without wrapping — only the decoder side is ever used at
 * runtime.
 */
final case class View[Cols <: Tuple, Name <: String & Singleton](
  name: Name,
  schema: Option[String],
  columns: Cols
) extends Relation[Cols] {

  /** A bare `View` is its own alias — parallel to `Table`. */
  type Alias = Name
  val currentAlias: Name        = name
  val expectedTableType: String = "VIEW"

  /** Place the view in a non-default schema. */
  def inSchema(s: String): View[Cols, Name] = copy(schema = Some(s))

  /** Primitive column-metadata rewrite (parallel to [[Table.withColumn]]). */
  inline def withColumn[N <: String & Singleton](inline n: N)(
    f: Column[Any, N, Boolean, Tuple] => Column[Any, N, Boolean, Tuple]
  ): View[Cols, Name] = {
    skunk.sharp.internal.CompileChecks.requireColumn[Cols, N]
    copy(columns = Table.updateCol[Cols, N](columns, n, f).asInstanceOf[Cols])
  }

  /** Override a column's skunk codec (parallel to [[Table.withColumnCodec]]). `tpe` is read from the codec. */
  inline def withColumnCodec[N <: String & Singleton, T](inline n: N, codec: Codec[T]): View[Cols, Name] =
    withColumn(n)(c => c.copy(tpe = PgTypes.typeOf(codec), codec = codec.asInstanceOf[skunk.Codec[Any]]))

}

object View {

  /** Entry point for the column-by-column view builder. */
  inline def builder[Name <: String & Singleton](name: Name): ViewBuilder[EmptyTuple, Name] =
    new ViewBuilder[EmptyTuple, Name](name, None, EmptyTuple)

  /**
   * Derive a [[View]] from a case class — quick-start path using default `PgTypeFor` per field. See the caveats on
   * [[Table.of]] about ambiguous defaults; prefer the builder for anything non-trivial.
   *
   * Uses the continuation pattern (see [[Table.of]]) so `View.of[T]("name")` can infer the name singleton after `T` is
   * given explicitly.
   */
  inline def of[T <: Product]: OfCont[T] = new OfCont[T]

  final class OfCont[T <: Product] {

    inline def apply[Name <: String & Singleton](viewName: Name)(using
      m: Mirror.ProductOf[T],
      dc: DeriveColumns[m.MirroredElemLabels, m.MirroredElemTypes]
    ): View[dc.Out, Name] = View[dc.Out, Name](viewName, None, dc.value)

  }

}

/** Column-by-column [[View]] builder — codec-first, parallel to [[TableBuilder]] minus the mutation-facing knobs. */
final class ViewBuilder[Cols <: Tuple, Name <: String & Singleton](
  val name: Name,
  val schema: Option[String],
  val columns: Cols
) {

  /** Non-nullable column. */
  inline def column[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): ViewBuilder[Tuple.Append[Cols, Column[T, N, false, EmptyTuple]], Name] = {
    val col = Column[T, N, false, EmptyTuple](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec,
      isNullable = false,
      attrs = Nil
    )
    new ViewBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, false, EmptyTuple]]]
    )
  }

  /** Nullable column (codec wrapped with `.opt` internally). */
  inline def columnOpt[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): ViewBuilder[Tuple.Append[Cols, Column[Option[T], N, true, EmptyTuple]], Name] = {
    val col = Column[Option[T], N, true, EmptyTuple](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec.opt,
      isNullable = true,
      attrs = Nil
    )
    new ViewBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, EmptyTuple]]]
    )
  }

  def inSchema(s: String): ViewBuilder[Cols, Name] = new ViewBuilder[Cols, Name](name, Some(s), columns)

  def build: View[Cols, Name] = View[Cols, Name](name, schema, columns)
}
