package skunk.sharp.contrib

import cats.syntax.either.*
import skunk.Codec
import skunk.data.Type
import skunk.sharp.pg.PgTypeFor

/**
 * Mix-in base for an opaque-text contrib tag (`citext`, `lquery`, `ltxtquery`, `pgvector` future tags, …). Encapsulates
 * the recurring codec + `PgTypeFor` + extension-name wiring so each new tag's companion is two lines:
 *
 * {{{
 *   opaque type Citext <: String = String
 *   object Citext extends TextTag[Citext]("citext", "citext") {
 *     protected def wrap(s: String): Citext = s   // legal here — alias is transparent inside the companion
 *   }
 * }}}
 *
 * Why an abstract `wrap` instead of a single `asInstanceOf` inside this trait: opaque types can only be constructed
 * inside their defining companion. Each tag's `wrap` body is the trivial `s => s` (legal there because the alias is
 * transparent at that scope); this base then uses it to build the codec.
 *
 * Wire format is plain Postgres text — `pgTypeName` is the type's Postgres identifier (`"citext"`, `"lquery"`, …) and
 * `requiredExtension` is the `CREATE EXTENSION` name passed through to [[PgTypeFor.requiredExtension]] for the schema
 * validator.
 */
abstract class TextTag[T <: String](val PgTypeName: String, val RequiredExtension: String) {

  protected def wrap(s: String): T

  /** Tag a `String` as `T`. Public alias for [[wrap]] — most callers should use this. */
  inline def apply(s: String): T = wrap(s)

  /** Skunk codec built on the Postgres text wire format. */
  val codec: Codec[T] =
    Codec.simple[T]((t: T) => t: String, s => wrap(s).asRight, Type(PgTypeName))

  given pgTypeFor: PgTypeFor[T] = PgTypeFor.instanceWithExtension(codec, RequiredExtension)

}
