package skunk.sharp.iron

import io.github.iltotore.iron.*
import skunk.Codec
import skunk.sharp.pg.PgTypeFor

/**
 * Integration with [Iron](https://iltotore.github.io/iron/).
 *
 * A refined type `A :| C` reuses the underlying `PgTypeFor[A]` — same skunk codec, constraint information is carried
 * only at the Scala level. The Postgres type is always read from the codec, so it automatically matches the base type.
 *
 * Decoding from the database does NOT re-validate the Iron constraint: we trust what Postgres gives us (application
 * invariants and DB constraints are assumed to keep data valid). If you want strict validation on decode, wrap the
 * provided codec with `Codec.emap` plus `refineEither`.
 */
given refinedPgTypeFor[A, C](using base: PgTypeFor[A]): PgTypeFor[A :| C] =
  PgTypeFor.instance(
    base.codec.imap[A :| C](_.asInstanceOf[A :| C])(_.asInstanceOf[A])
  )

object syntax {

  /**
   * Given an Iron-refined `Codec[A :| C]`, downcast to a `Codec[A]` for interop with legacy code that needs the plain
   * base type.
   */
  extension [A, C](c: Codec[A :| C]) def toBaseCodec: Codec[A] = c.asInstanceOf[Codec[A]]
}
