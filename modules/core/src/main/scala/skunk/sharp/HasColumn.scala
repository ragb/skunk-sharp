package skunk.sharp

/**
 * Type-level predicate: reduces to `true` iff `Cols` contains a [[Column]] whose singleton-type name equals `N`.
 *
 * Used by constraint modifiers (`withPrimary`, `withDefault`, `withUnique`) to refuse column names at compile time that
 * aren't part of the table's declared shape.
 */
type HasColumn[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, n, d, p, u] *: tail => true
  case h *: tail                        => HasColumn[tail, N]
  case EmptyTuple                       => false
}

/**
 * Type-level lookup: resolves to the [[Column]] in `Cols` whose singleton-type name equals `N`. Does not reduce when
 * `N` is absent — that "stuck" match type produces a compile error at the use site.
 */
type ColumnAt[Cols <: Tuple, N <: String & Singleton] = Cols match {
  case Column[t, N, n, d, p, u] *: tail => Column[t, N, n, d, p, u]
  case h *: tail                        => ColumnAt[tail, N]
}

/** Type-level extraction: the Scala value type of the column named `N` in `Cols`. */
type ColumnType[Cols <: Tuple, N <: String & Singleton] = Cols match {
  case Column[t, N, n, d, p, u] *: tail => t
  case h *: tail                        => ColumnType[tail, N]
}

/** Type-level extraction: the nullability flag of the column named `N` in `Cols`. */
type ColumnNullable[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, n, d, p, u] *: tail => n
  case h *: tail                        => ColumnNullable[tail, N]
}

/** Type-level extraction: the has-default flag of the column named `N` in `Cols`. */
type ColumnDefault[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, n, d, p, u] *: tail => d
  case h *: tail                        => ColumnDefault[tail, N]
}

/** Type-level membership check — reduces to `true` if `T` is element of `Xs`. */
type Contains[T, Xs <: Tuple] <: Boolean = Xs match {
  case EmptyTuple => false
  case T *: tail  => true
  case h *: tail  => Contains[T, tail]
}

/** Reduces to `true` iff every name in `Ns` is a declared column name in `Cols`. */
type AllNamesInCols[Ns <: Tuple, Cols <: Tuple] <: Boolean = Ns match {
  case EmptyTuple => true
  case n *: tail  => HasColumn[Cols, n & String & Singleton] match {
      case true  => AllNamesInCols[tail, Cols]
      case false => false
    }
}

/**
 * Reduces to `true` iff every *required* column in `Cols` (`Default = false`) has its name in `Ns`. Required columns
 * cannot be omitted from an INSERT; defaulted ones can.
 */
type CoversRequired[Cols <: Tuple, Ns <: Tuple] <: Boolean = Cols match {
  case EmptyTuple                            => true
  case Column[t, n, nu, false, p, u] *: tail => Contains[n, Ns] match {
      case true  => CoversRequired[tail, Ns]
      case false => false
    }
  case Column[t, n, nu, true, p, u] *: tail => CoversRequired[tail, Ns]
}

/**
 * Type-level predicate: reduces to `true` iff the column named `N` in `Cols` is declared `.withPrimary` or
 * `.withUnique`. Powers compile-time evidence for `.onConflict(col)` — Postgres requires the conflict target to be
 * backed by a unique or exclusion constraint.
 */
type HasUniqueness[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, d, true, u] *: tail => true
  case Column[t, N, nu, d, p, true] *: tail => true
  case Column[t, N, nu, d, p, u] *: tail    => false
  case h *: tail                            => HasUniqueness[tail, N]
  case EmptyTuple                           => false
}
