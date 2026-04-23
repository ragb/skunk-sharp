package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment}
import skunk.sharp.{NamedRowOf, TypedExpr}
import skunk.sharp.internal.{rowCodec, RawConstants}
import skunk.util.Origin

/**
 * End-to-end typed path for a single-source SELECT with one typed WHERE predicate. This is the first
 * demonstration that [[SelectBuilder]]'s rendering can thread `Args` at the type level all the way to
 * [[CompiledQuery]][`Args, R`], so the user can ascribe
 * `val q: CompiledQuery[Int, User] = users.select.whereTyped(…).compileTyped`.
 *
 * Today only a single `whereTyped` call is supported — the path accepts a pre-built `TypedWhere[A]`, typically
 * produced by `SqlMacros.infixTyped` from an explicit call. Subsequent typed-predicate combinators (`&&`, `||`,
 * multiple `.whereTyped` calls with cumulative `Args`) and typed variants of `.select(f)` / `.orderBy` /
 * `.limit` / etc. land on the same pattern but expand the builder's type surface substantially — that work is
 * tracked as the remaining Phase B surgery.
 */
final class TypedSelectEnd[Ss <: Tuple, Args] private[dsl] (
  private val base:           SelectBuilder[Ss],
  private val whereFragment:  Fragment[Args],
  private val whereArgs:      Args
) {

  /**
   * Compile the typed chain into a [[CompiledQuery]] with concrete visible `Args`. Works for a single-source
   * SELECT over a [[skunk.sharp.Table]] / [[skunk.sharp.View]]; multi-source joins need the projected-select
   * variant which isn't wired yet.
   *
   * Rendering is manual here rather than going through `SelectBuilder.compileFragment`: that path produces an
   * `AppliedFragment` whose `A` is existential. To preserve `Args`, we assemble the SELECT body ourselves,
   * interleaving the cached structural `AppliedFragment`s (which are `AppliedFragment`s over `Void`) with the
   * typed `whereFragment`'s `parts` list.
   */
  def compile(using ev: IsSingleSource[Ss]): CompiledQuery[Args, NamedRowOf[ev.Cols]] = {
    val entries = base.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
    val head    = entries.head

    // SELECT <cols> FROM <table> — all Void-encoded AppliedFragments.
    val projList: AppliedFragment = RawConstants.SELECT |+| TypedExpr.raw("*") |+| RawConstants.FROM |+|
      aliasedFromEntry(head)

    val wherePrefix: AppliedFragment = RawConstants.WHERE

    // Assemble parts: `SELECT * FROM …  WHERE ` + whereFragment.parts. The prefix holds Void parameters (none);
    // only whereFragment contributes to the final `Args` encoder.
    val combinedParts =
      projList.fragment.parts ++ wherePrefix.fragment.parts ++ whereFragment.parts

    val combinedFragment: Fragment[Args] =
      Fragment(combinedParts, whereFragment.encoder, Origin.unknown)

    val codec: Codec[NamedRowOf[ev.Cols]] =
      rowCodec(head.effectiveCols).asInstanceOf[Codec[NamedRowOf[ev.Cols]]]

    CompiledQuery.mk[Args, NamedRowOf[ev.Cols]](combinedFragment, whereArgs, codec)
  }

}

