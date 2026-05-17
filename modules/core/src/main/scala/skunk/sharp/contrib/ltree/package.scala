package skunk.sharp.contrib

/**
 * `ltree` — Postgres's hierarchical label-path type. The Scala-side value type ([[LTree]]) is intentionally **our own**
 * opaque `<: String` tag rather than a re-export of [[skunk.data.LTree]] (which skunk also ships). Two reasons:
 *
 *   - The `<: String` subtyping lets every string operator that uses `Stripped[T] <:< String` evidence — `ilike`,
 *     `like`, the trigram operators in [[skunk.sharp.contrib.pgtrgm.ops]], …— apply directly to an `LTree`-typed
 *     expression. Skunk's `skunk.data.LTree` is not a `String` subtype, so those operators would refuse it.
 *   - Implicit / `given` scope: the `PgTypeFor[LTree]` carrying the required-extension hint must live in either the
 *     companion of `LTree` or the companion of one of its supertypes for `Table.of[T]` / inferred-codec column paths to
 *     find it automatically. We can only put it on our own `LTree` — we don't own `skunk.data.LTree`'s companion.
 *
 * Wire format and semantics are otherwise identical. If you have an existing pile of `skunk.data.LTree` values, the
 * conversion is just `LTree(d.toString)` / `skunk.data.LTree.fromString(myLTree)`.
 */
package object ltree
