package skunk.sharp.fts

import skunk.Codec
import skunk.data.Type
import skunk.sharp.pg.PgTypeFor

/**
 * Postgres full-text search types. Built into Postgres — no extension. Values usually come from functions
 * (`Fts.toTsVector`, `Fts.websearchToTsQuery`, …) rather than being written by hand; the text form is exposed as a
 * `String` subtype.
 *
 * Like the other type tags, `apply` is unchecked: it wraps text in Postgres's `tsvector` / `tsquery` syntax as-is and
 * Postgres parses it when the value is used.
 */
opaque type TsVector <: String = String

object TsVector {

  inline def apply(s: String): TsVector = s

  val codec: Codec[TsVector] = Codec.simple[TsVector](v => v, s => Right(s), Type("tsvector"))

  given PgTypeFor[TsVector] = PgTypeFor.instance(codec)

}

/** A full-text query (`tsquery`) — see [[TsVector]]. */
opaque type TsQuery <: String = String

object TsQuery {

  inline def apply(s: String): TsQuery = s

  val codec: Codec[TsQuery] = Codec.simple[TsQuery](q => q, s => Right(s), Type("tsquery"))

  given PgTypeFor[TsQuery] = PgTypeFor.instance(codec)

}
