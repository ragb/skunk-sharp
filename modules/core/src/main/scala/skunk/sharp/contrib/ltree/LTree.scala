package skunk.sharp.contrib.ltree

import skunk.Codec
import skunk.codec.all as pg
import skunk.data
import skunk.sharp.contrib.TextTag
import skunk.sharp.pg.PgTypeFor

/**
 * `ltree` — hierarchical label tree (`top.science.astronomy`). Requires `CREATE EXTENSION ltree;`.
 *
 * `LTree <: String` so it flows through every `Stripped[T] <:< String`-gated operator (`ilike`, `like`, trigram, …) and
 * lives in implicit scope for `Table.of[T]` derivation. **Underlying validation and codec delegate to skunk**: `apply`
 * / `wrap` runs every string through `skunk.data.LTree.fromString` so an invalid path is rejected at construction time,
 * and the [[codec]] piggybacks on skunk's bundled `pg.ltree` codec via `.imap` — no second parser to keep in sync.
 */
opaque type LTree <: String = String

object LTree {

  val RequiredExtension: String = "ltree"

  /**
   * Validate via skunk's parser, then return the canonical text form as an `LTree`. Throws `IllegalArgumentException`
   * on a bad path — matches the failure mode of [[skunk.data.LTree.fromString]] used as a total constructor.
   */
  def apply(s: String): LTree =
    data.LTree.fromString(s).fold(
      e => throw new IllegalArgumentException(s"invalid ltree '$s': $e"),
      _.toString
    )

  /**
   * Skunk's `pg.ltree` codec (which decodes to `skunk.data.LTree`) re-mapped to our tag's canonical text form. We keep
   * one parser path — skunk's — for both Scala-side `.apply` and on-the-wire decoding.
   */
  val codec: Codec[LTree] =
    pg.ltree.imap[LTree](_.toString)(s =>
      data.LTree.fromString(s).fold(e => throw new IllegalArgumentException(s"invalid ltree '$s': $e"), identity)
    )

  given PgTypeFor[LTree] = PgTypeFor.instanceWithExtension(codec, RequiredExtension)

}

/**
 * `lquery` — ltree-pattern matched against an `ltree` value (e.g. `top.*.astronomy{1,2}`). Wire format is text and
 * skunk doesn't ship a dedicated codec, so this one uses the shared [[TextTag]] base with no extra validation.
 */
opaque type LQuery <: String = String

object LQuery extends TextTag[LQuery]("lquery", "ltree") {
  protected def wrap(s: String): LQuery = s
}

/** `ltxtquery` — boolean ltree text query. Same wire / tag shape as [[LQuery]]. */
opaque type LTxtQuery <: String = String

object LTxtQuery extends TextTag[LTxtQuery]("ltxtquery", "ltree") {
  protected def wrap(s: String): LTxtQuery = s
}
