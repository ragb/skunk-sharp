package skunk.sharp.dsl

import skunk.sharp.{AliasedExpr, Column, TypedColumn}

/**
 * Type-level map from a projection tuple (as tracked by [[ProjectedSelect]]'s `Proj` phantom) to the tuple of
 * [[Column]] descriptors it represents. Drives [[ProjectedSelect.alias]] — only projections with a known name (either a
 * bare column reference or an `.as("name")`-aliased expression) can become named columns of a joinable relation.
 * Arbitrary un-named expressions (`u.email ++ u.suffix`, `Pg.lower(u.email)`, …) have no stable name Postgres is
 * obliged to use, so they can't be surfaced as a column — the compile-time guard below rejects them.
 *
 *   - `TypedColumn[T, Null, N]` → `Column[T, N, Null, EmptyTuple]` (preserves nullability at the phantom).
 *   - `AliasedExpr[T, N]` → `Column[T, N, false, EmptyTuple]` (explicit alias; value's own nullability isn't
 *     reintroduced here — `Option[_]` outputs still decode as `Option[_]` via the codec, but the column's `Null`
 *     phantom is `false` because there's no declared-nullable column behind the alias).
 */
type ProjCols[Proj <: Tuple] <: Tuple = Proj match {
  case EmptyTuple                    => EmptyTuple
  case TypedColumn[t, nu, n] *: rest => Column[t, n, nu, EmptyTuple] *: ProjCols[rest]
  case AliasedExpr[t, n, ?] *: rest  => Column[t, n, false, EmptyTuple] *: ProjCols[rest]
}

/**
 * Evidence that every element of `Proj` is either a `TypedColumn[_, _, _]` or an `AliasedExpr[_, _]`. Summoned by
 * [[ProjectedSelect.alias]] so non-named projections produce a pointed compile error rather than a cryptic
 * match-type-reduction failure.
 */
@scala.annotation.implicitNotFound(
  "skunk-sharp: `.alias(\"…\")` on a projected SELECT requires every projected element to be a bare column " +
    "reference or an expression aliased via `.as(\"name\")`. Found projections that have no stable name — pick " +
    "columns (`u.email`), or add aliases (`Pg.lower(u.email).as(\"lower_email\")`)."
)
sealed trait AllNamedProj[Proj <: Tuple]

object AllNamedProj {

  given empty: AllNamedProj[EmptyTuple] = new AllNamedProj[EmptyTuple] {}

  given consColumn[T, Nu <: Boolean, N <: String & Singleton, Rest <: Tuple](using
    rest: AllNamedProj[Rest]
  ): AllNamedProj[TypedColumn[T, Nu, N] *: Rest] =
    new AllNamedProj[TypedColumn[T, Nu, N] *: Rest] {}

  given consAliased[T, N <: String & Singleton, A, Rest <: Tuple](using
    rest: AllNamedProj[Rest]
  ): AllNamedProj[AliasedExpr[T, N, A] *: Rest] =
    new AllNamedProj[AliasedExpr[T, N, A] *: Rest] {}

}
