package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.Codec
import skunk.sharp.pg.PgTypeFor

/**
 * Tag types disambiguating Postgres's `json` and `jsonb` on a column's Scala-side Type. Both carry [[io.circe.Json]]
 * values at runtime (same wire format, same decoder), but the DSL picks the right codec based on which tag a column /
 * expression declares — same pattern as `Varchar[N]` / `Text` for strings.
 *
 * Use them in case classes consumed by `Table.of[T]` / `View.of[T]`:
 *
 * {{{
 *   import skunk.sharp.json.*
 *
 *   case class Document(id: UUID, body: Jsonb, metadata: Json)
 *   val docs = Table.of[Document]("documents")
 * }}}
 *
 * Prefer [[Jsonb]] for new schema work — `jsonb` in Postgres is the indexable, operator-rich variant; `json` preserves
 * the source text and is rarely what you want. Most operators in this module are defined on `TypedExpr[Jsonb]` only.
 */

/** Postgres `json` (text-preserving). Use [[Jsonb]] for indexed / operator-rich columns. */
opaque type Json <: CirceJson = CirceJson

object Json {

  /** Construct a `Json`-tagged value from a circe `Json`. */
  def apply(j: CirceJson): Json = j

  /** Unwrap to the underlying circe `Json`. */
  extension (j: Json) def unwrap: CirceJson = j

  given PgTypeFor[Json] = PgTypeFor.instance(codecs.json.asInstanceOf[Codec[Json]])

}

/** Postgres `jsonb` (binary, indexable, operator-rich). Most operators in this module target this tag. */
opaque type Jsonb <: CirceJson = CirceJson

/**
 * Namespace for `Jsonb` — combines the tag companion (construct / unwrap / `PgTypeFor`) with the full [[PgJsonb]] trait
 * so `Jsonb.toJsonb`, `Jsonb.jsonbSet`, etc. are all available without extra imports.
 */
object Jsonb extends PgJsonb {

  /** Construct a `Jsonb`-tagged value from a circe `Json`. */
  def apply(j: CirceJson): Jsonb = j

  /** Unwrap to the underlying circe `Json`. */
  extension (j: Jsonb) def unwrap: CirceJson = j

  given PgTypeFor[Jsonb] = PgTypeFor.instance(codecs.jsonb.asInstanceOf[Codec[Jsonb]])

}
