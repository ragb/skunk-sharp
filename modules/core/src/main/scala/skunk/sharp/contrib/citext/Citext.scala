package skunk.sharp.contrib.citext

import skunk.sharp.contrib.TextTag

/**
 * `citext` — case-insensitive text. Comparison, hashing, `=` / `LIKE` are all case-insensitive at the storage layer,
 * so declaring `email: Citext` (instead of bare `String`) gives Scala-side type safety without any per-call
 * `lower(...)` ceremony.
 *
 * Requires `CREATE EXTENSION citext;` — picked up by the schema validator automatically through the `PgTypeFor`
 * registered on the companion.
 *
 * `Citext <: String` — flows through existing string operators (`===`, `like`, `ilike`, trigram ops, `++`, …) with no
 * extra plumbing.
 */
opaque type Citext <: String = String

object Citext extends TextTag[Citext]("citext", "citext") {
  protected def wrap(s: String): Citext = s
}
