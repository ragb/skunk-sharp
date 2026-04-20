package skunk.sharp

/**
 * Type-level predicate: reduces to `true` iff `Cols` contains a [[Column]] whose singleton-type name equals `N`.
 *
 * Used by constraint modifiers (`withPrimary`, `withDefault`, `withUnique`) to refuse column names at compile time that
 * aren't part of the table's declared shape.
 */
type HasColumn[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, attrs] *: tail => true
  case h *: tail                       => HasColumn[tail, N]
  case EmptyTuple                      => false
}

/**
 * Type-level lookup: resolves to the [[Column]] in `Cols` whose singleton-type name equals `N`. Does not reduce when
 * `N` is absent — that "stuck" match type produces a compile error at the use site.
 */
type ColumnAt[Cols <: Tuple, N <: String & Singleton] = Cols match {
  case Column[t, N, nu, attrs] *: tail => Column[t, N, nu, attrs]
  case h *: tail                       => ColumnAt[tail, N]
}

/** Type-level extraction: the Scala value type of the column named `N` in `Cols`. */
type ColumnType[Cols <: Tuple, N <: String & Singleton] = Cols match {
  case Column[t, N, nu, attrs] *: tail => t
  case h *: tail                       => ColumnType[tail, N]
}

/** Type-level extraction: the nullability flag of the column named `N` in `Cols`. */
type ColumnNullable[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, attrs] *: tail => nu
  case h *: tail                       => ColumnNullable[tail, N]
}

/** Type-level extraction: the attribute tuple of the column named `N` in `Cols`. */
type ColumnAttrs[Cols <: Tuple, N <: String & Singleton] <: Tuple = Cols match {
  case Column[t, N, nu, attrs] *: tail => attrs
  case h *: tail                       => ColumnAttrs[tail, N]
}

/** Type-level extraction: whether the column named `N` in `Cols` has a database-side default. */
type ColumnDefault[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, attrs] *: tail => Contains[ColumnAttr.Default, attrs]
  case h *: tail                       => ColumnDefault[tail, N]
}

/** Type-level membership check — reduces to `true` if `T` is element of `Xs`. */
type Contains[T, Xs <: Tuple] <: Boolean = Xs match {
  case EmptyTuple => false
  case T *: tail  => true
  case h *: tail  => Contains[T, tail]
}

/** Boolean disjunction at the type level. */
type Or[A <: Boolean, B <: Boolean] <: Boolean = A match {
  case true  => true
  case false => B
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
 * Reduces to `true` iff every *required* column in `Cols` (no `ColumnAttr.Default` marker) has its name in `Ns`.
 * Required columns cannot be omitted from an INSERT; defaulted ones can.
 */
type CoversRequired[Cols <: Tuple, Ns <: Tuple] <: Boolean = Cols match {
  case EmptyTuple                      => true
  case Column[t, n, nu, attrs] *: tail => Contains[ColumnAttr.Default, attrs] match {
      case true  => CoversRequired[tail, Ns]
      case false => Contains[n, Ns] match {
          case true  => CoversRequired[tail, Ns]
          case false => false
        }
    }
}

/**
 * Type-level predicate: reduces to `true` iff the column named `N` in `Cols` is declared `.withPrimary` or
 * `.withUnique` (or both). Powers compile-time evidence for `.onConflict(c => c.<n>)` — Postgres requires the conflict
 * target to be backed by a unique or exclusion constraint, and a PRIMARY KEY is implicitly unique.
 */
type HasUniqueness[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, attrs] *: tail =>
    Or[Contains[ColumnAttr.Primary, attrs], Contains[ColumnAttr.Unique, attrs]]
  case h *: tail  => HasUniqueness[tail, N]
  case EmptyTuple => false
}
