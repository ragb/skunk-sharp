package skunk.sharp.iron

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.collection.{FixedLength, MaxLength}
import skunk.Codec
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.pg.tags.{Bpchar, Varchar}

/**
 * Integration with [Iron](https://iltotore.github.io/iron/).
 *
 * Two levels of support:
 *
 *   - The **fallback** `refinedPgTypeFor[A, C]` reuses the base `PgTypeFor[A]` — constraint is Scala-only, the codec
 *     and Postgres type mirror the underlying unrefined type. This is what kicks in for constraints that have no DB
 *     counterpart (`Positive`, `Match[…]`, `Greater[N]`, etc.).
 *   - **Bridges** from common Iron constraints to skunk-sharp's tag types in core (see `skunk.sharp.pg.tags`). Example:
 *     `String :| MaxLength[256]` picks the `varchar(256)` codec by routing to `PgTypeFor[Varchar[256]]`. Bridges are
 *     more specific than the fallback, so given-resolution prefers them automatically.
 *
 * Decoding does NOT re-validate the Iron constraint — we trust what Postgres returns. Wrap with `Codec.emap` +
 * `refineEither` if you want strict validation on decode.
 */
given refinedPgTypeFor[A, C](using base: PgTypeFor[A]): PgTypeFor[A :| C] =
  PgTypeFor.instance(
    base.codec.imap[A :| C](_.asInstanceOf[A :| C])(_.asInstanceOf[A])
  )

/** `String :| MaxLength[N]` ⇒ `varchar(n)`. More specific than the fallback; implicit resolution prefers it. */
given varcharFromMaxLength[N <: Int](using pf: PgTypeFor[Varchar[N]]): PgTypeFor[String :| MaxLength[N]] =
  PgTypeFor.instance(pf.codec.asInstanceOf[Codec[String :| MaxLength[N]]])

/** `String :| FixedLength[N]` ⇒ `bpchar(n)`. */
given bpcharFromFixedLength[N <: Int](using pf: PgTypeFor[Bpchar[N]]): PgTypeFor[String :| FixedLength[N]] =
  PgTypeFor.instance(pf.codec.asInstanceOf[Codec[String :| FixedLength[N]]])

object syntax {

  /**
   * Given an Iron-refined `Codec[A :| C]`, downcast to a `Codec[A]` for interop with legacy code that needs the plain
   * base type.
   */
  extension [A, C](c: Codec[A :| C]) def toBaseCodec: Codec[A] = c.asInstanceOf[Codec[A]]
}
