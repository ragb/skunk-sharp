package skunk.sharp

import skunk.Codec
import skunk.sharp.internal.{deriveColumns, ColumnsFromMirror}
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
final case class View[Cols <: Tuple](
  name: String,
  schema: Option[String],
  columns: Cols
) extends Relation[Cols] {

  /** Path-dependent singleton of the view's name, used by JOIN extensions as the default alias. */
  type Name = name.type

  val expectedTableType: String = "VIEW"

  /** Place the view in a non-default schema. */
  def inSchema(s: String): View[Cols] = copy(schema = Some(s))

  /** Primitive column-metadata rewrite (parallel to [[Table.withColumn]]). */
  inline def withColumn[N <: String & Singleton](inline n: N)(
    f: Column[Any, N, Boolean, Boolean] => Column[Any, N, Boolean, Boolean]
  ): View[Cols] = {
    skunk.sharp.internal.CompileChecks.requireColumn[Cols, N]
    copy(columns = Table.updateCol[Cols, N](columns, n, f).asInstanceOf[Cols])
  }

  /** Override a column's skunk codec (parallel to [[Table.withColumnCodec]]). `tpe` is read from the codec. */
  inline def withColumnCodec[N <: String & Singleton, T](inline n: N, codec: Codec[T]): View[Cols] =
    withColumn(n)(c => c.copy(tpe = PgTypes.typeOf(codec), codec = codec.asInstanceOf[skunk.Codec[Any]]))

}

object View {

  /** Entry point for the column-by-column view builder. */
  def builder(name: String): ViewBuilder[EmptyTuple] =
    new ViewBuilder[EmptyTuple](name, None, EmptyTuple)

  /**
   * Derive a [[View]] from a case class — quick-start path using default `PgTypeFor` per field. See the caveats on
   * [[Table.of]] about ambiguous defaults; prefer the builder for anything non-trivial.
   */
  inline def of[T <: Product](viewName: String)(using
    m: Mirror.ProductOf[T]
  ): View[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes]] = {
    val cols = deriveColumns[m.MirroredElemLabels, m.MirroredElemTypes]
    View[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes]](
      viewName,
      None,
      cols.asInstanceOf[ColumnsFromMirror[m.MirroredElemLabels, m.MirroredElemTypes]]
    )
  }

}

/** Column-by-column [[View]] builder — codec-first, parallel to [[TableBuilder]] minus the mutation-facing knobs. */
final class ViewBuilder[Cols <: Tuple](
  val name: String,
  val schema: Option[String],
  val columns: Cols
) {

  /** Non-nullable column. */
  inline def column[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): ViewBuilder[Tuple.Append[Cols, Column[T, N, false, false]]] = {
    val col = Column[T, N, false, false](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec,
      isNullable = false,
      hasDefault = false
    )
    new ViewBuilder(name, schema, (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[T, N, false, false]]])
  }

  /** Nullable column (codec wrapped with `.opt` internally). */
  inline def columnOpt[T, N <: String & Singleton](
    n: N,
    codec: Codec[T]
  ): ViewBuilder[Tuple.Append[Cols, Column[Option[T], N, true, false]]] = {
    val col = Column[Option[T], N, true, false](
      name = n,
      tpe = PgTypes.typeOf(codec),
      codec = codec.opt,
      isNullable = true,
      hasDefault = false
    )
    new ViewBuilder(
      name,
      schema,
      (columns :* col).asInstanceOf[Tuple.Append[Cols, Column[Option[T], N, true, false]]]
    )
  }

  def inSchema(s: String): ViewBuilder[Cols] = new ViewBuilder[Cols](name, Some(s), columns)

  def build: View[Cols] = View[Cols](name, schema, columns)
}
