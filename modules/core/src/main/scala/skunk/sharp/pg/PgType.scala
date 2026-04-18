package skunk.sharp.pg

import skunk.data.Type

/**
 * Helpers around [[skunk.data.Type]] — we lean on skunk's own type registry (`skunk.data.Type` plus `skunk.util.Typer`)
 * rather than maintain a parallel enum.
 *
 * The DSL stores the Postgres type on a column as a `skunk.data.Type` read from the codec (via `codec.types.head`). The
 * only thing we add here is a lookup from skunk's short canonical names (`int4`, `varchar`, `bpchar`, …) to the verbose
 * strings that appear in `information_schema.columns.data_type` (`integer`, `character varying`, `character`, …) — used
 * by the schema validator to compare declared vs. actual column types.
 */
object PgTypes {

  /**
   * Skunk `Type` for a codec. Skunk's `Codec.types: List[Type]` is a list because a codec can span multiple database
   * columns — twiddled / product codecs like `int4 ~ text` or `a *: b *: c` concatenate their types. `Column` in our
   * DSL models exactly one DB column, so we require a single-column codec here and fail fast if the caller hands us a
   * multi-column one (the tail types would otherwise be silently dropped).
   */
  def typeOf(c: skunk.Codec[?]): Type = c.types match {
    case one :: Nil => one
    case Nil        =>
      throw new IllegalArgumentException(
        "skunk-sharp: codec has no skunk.data.Type — cannot be used as a single-column codec."
      )
    case more =>
      throw new IllegalArgumentException(
        s"skunk-sharp: expected a single-column codec, got one with ${more.size} types: " +
          more.map(_.name).mkString("[", ", ", "]") +
          ". Tuple/product codecs (a ~ b, a *: b *: c) span multiple DB columns; pass one column at a time to " +
          "Table.builder.column."
      )
  }

  /** Short type name (no parameters) — `"varchar"` for `varchar(256)`, `"numeric"` for `numeric(10,2)`. */
  def shortName(t: Type): String = {
    val n = t.name
    n.indexOf('(') match {
      case -1 => n
      case i  => n.take(i)
    }
  }

  /** Mapping from skunk's short Postgres type name to the value `information_schema.columns.data_type` reports. */
  val informationSchemaDataType: Map[String, String] = Map(
    "bool"        -> "boolean",
    "int2"        -> "smallint",
    "int4"        -> "integer",
    "int8"        -> "bigint",
    "float4"      -> "real",
    "float8"      -> "double precision",
    "numeric"     -> "numeric",
    "varchar"     -> "character varying",
    "bpchar"      -> "character",
    "text"        -> "text",
    "name"        -> "name",
    "bytea"       -> "bytea",
    "uuid"        -> "uuid",
    "date"        -> "date",
    "time"        -> "time without time zone",
    "timetz"      -> "time with time zone",
    "timestamp"   -> "timestamp without time zone",
    "timestamptz" -> "timestamp with time zone",
    "interval"    -> "interval",
    "bit"         -> "bit",
    "varbit"      -> "bit varying",
    "json"        -> "json",
    "jsonb"       -> "jsonb",
    "xml"         -> "xml",
    "money"       -> "money",
    "inet"        -> "inet",
    "cidr"        -> "cidr",
    "macaddr"     -> "macaddr",
    "macaddr8"    -> "macaddr8",
    "tsvector"    -> "tsvector",
    "tsquery"     -> "tsquery"
  )

  /**
   * The `information_schema.columns.data_type` string a column of the given skunk `Type` will carry. Unknown types fall
   * back to the short name.
   */
  def dataType(t: Type): String =
    informationSchemaDataType.getOrElse(shortName(t), shortName(t))

  /**
   * Name to use for a SQL cast (`expr::<name>`). Defaults to the short skunk name — these are the short forms Postgres
   * itself accepts in `::` casts (`int8`, `varchar`, `timestamptz`).
   */
  def castName(t: Type): String = shortName(t)
}
