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

/** Reduces to `true` iff every element of `Xs` is present in `Ys`. */
type AllInTuple[Xs <: Tuple, Ys <: Tuple] <: Boolean = Xs match {
  case EmptyTuple => true
  case h *: tail  => Contains[h, Ys] match {
      case true  => AllInTuple[tail, Ys]
      case false => false
    }
}

/**
 * Set-equality on tuples — `true` iff `A` and `B` contain the same elements (order-independent). Used by
 * [[HasCompositeUniqueness]] to accept `.onConflict(c => (c.a, c.b))` against a declared
 * `.withCompositePrimary(("b", "a"))` regardless of declaration order.
 */
type TupleSetEq[A <: Tuple, B <: Tuple] <: Boolean = AllInTuple[A, B] match {
  case true  => AllInTuple[B, A]
  case false => false
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
 * Scan a column's `Attrs` tuple for any `Pk[Members]` / `Uq[Name, Members]` marker whose `Members` is the singleton
 * tuple `(N)` — i.e. the column itself is an entire primary-key or unique constraint group. Powers single-column
 * `.onConflict(c => c.<N>)` evidence.
 */
type HasSingletonKey[Attrs <: Tuple, N <: String & Singleton] <: Boolean = Attrs match {
  case EmptyTuple                                => false
  case ColumnAttr.Pk[N *: EmptyTuple] *: tail    => true
  case ColumnAttr.Uq[n, N *: EmptyTuple] *: tail => true
  case h *: tail                                 => HasSingletonKey[tail, N]
}

/**
 * Type-level predicate: reduces to `true` iff the column named `N` in `Cols` is a single-column primary-key or unique
 * constraint target. A column that is only a *part* of a composite PK / unique does **not** satisfy this — Postgres
 * won't accept a partial-constraint target in `ON CONFLICT`. Use [[HasCompositeUniqueness]] for the composite case.
 */
type HasUniqueness[Cols <: Tuple, N <: String & Singleton] <: Boolean = Cols match {
  case Column[t, N, nu, attrs] *: tail => HasSingletonKey[attrs, N]
  case h *: tail                       => HasUniqueness[tail, N]
  case EmptyTuple                      => false
}

/** Scan a column's `Attrs` for a `Pk[Members]` or `Uq[_, Members]` marker whose `Members` is set-equal to `Ns`. */
type HasGroupMatching[Attrs <: Tuple, Ns <: Tuple] <: Boolean = Attrs match {
  case EmptyTuple                        => false
  case ColumnAttr.Pk[members] *: tail    => TupleSetEq[members, Ns] match {
      case true  => true
      case false => HasGroupMatching[tail, Ns]
    }
  case ColumnAttr.Uq[n, members] *: tail => TupleSetEq[members, Ns] match {
      case true  => true
      case false => HasGroupMatching[tail, Ns]
    }
  case h *: tail                         => HasGroupMatching[tail, Ns]
}

/**
 * Composite-target evidence: `true` iff any column in `Cols` carries a `Pk[Members]` or `Uq[_, Members]` marker whose
 * `Members` is set-equal to `Ns`. Powers `.onConflict(c => (c.a, c.b, …))` — the full tuple of columns must exactly
 * match a declared composite primary-key or unique constraint (Postgres rejects subsets / supersets).
 *
 * Because every column in a declared group carries the same `Members` tuple, finding it on *any* column proves the
 * group was declared.
 */
type HasCompositeUniqueness[Cols <: Tuple, Ns <: Tuple] <: Boolean = Cols match {
  case Column[t, n, nu, attrs] *: tail =>
    HasGroupMatching[attrs, Ns] match {
      case true  => true
      case false => HasCompositeUniqueness[tail, Ns]
    }
  case EmptyTuple => false
}

/**
 * Extract column-name singletons from a tuple of GROUP BY expressions. Only bare [[TypedColumn]] references contribute
 * a name — expressions, function calls, literals skip. Used by [[GroupCoverage]] to collect the set of column names a
 * caller's GROUP BY provides.
 */
type GroupNames[G <: Tuple] <: Tuple = G match {
  case EmptyTuple                    => EmptyTuple
  case TypedColumn[t, nu, n] *: tail => n *: GroupNames[tail]
  case h *: tail                     => GroupNames[tail]
}

/**
 * Marker typeclass: evidence that `E` is a bare [[TypedColumn]] reference with singleton name `N`. Powers
 * [[GroupCoverage]]'s typeclass-based dispatch — match-type dispatch can't distinguish `TypedColumn` from
 * `TypedExpr` since the former extends the latter (match types require *provable* disjointness, which subtype
 * relationships defeat).
 */
sealed trait IsTypedCol[E] {
  type N <: String & Singleton
}

object IsTypedCol {

  type Aux[E, N0 <: String & Singleton] = IsTypedCol[E] { type N = N0 }

  given fromTypedColumn[T, Null <: Boolean, N_ <: String & Singleton]
    : IsTypedCol.Aux[TypedColumn[T, Null, N_], N_] = new IsTypedCol[TypedColumn[T, Null, N_]] {
    type N = N_
  }

}

/**
 * Coverage witness for a single projection element:
 *   - If `E` is a `TypedColumn[_, _, N]` whose `N` is in `GNames` — covered.
 *   - If `E` is *not* a column (aggregate, literal, aliased expression, …) — covered unconditionally.
 *   - If `E` is a column whose name is NOT in `GNames` — no instance resolves → compile error.
 */
sealed trait ElemCoverage[E, GNames <: Tuple]

object ElemCoverage {

  given columnCovered[E, N <: String & Singleton, GNames <: Tuple](using
    col: IsTypedCol.Aux[E, N],
    ev: Contains[N, GNames] =:= true
  ): ElemCoverage[E, GNames] = new ElemCoverage[E, GNames] {}

  given nonColumn[E, GNames <: Tuple](using
    nc: scala.util.NotGiven[IsTypedCol[E]]
  ): ElemCoverage[E, GNames] = new ElemCoverage[E, GNames] {}

}

/**
 * Every element of `Proj` has [[ElemCoverage]] under `GNames`. Walks the projection tuple inductively — given instance
 * resolution handles each element individually.
 */
sealed trait AllCovered[Proj <: Tuple, GNames <: Tuple]

object AllCovered {

  given empty[GNames <: Tuple]: AllCovered[EmptyTuple, GNames] = new AllCovered[EmptyTuple, GNames] {}

  given cons[H, Rest <: Tuple, GNames <: Tuple](using
    h: ElemCoverage[H, GNames],
    r: AllCovered[Rest, GNames]
  ): AllCovered[H *: Rest, GNames] = new AllCovered[H *: Rest, GNames] {}

}

/**
 * Compile-time GROUP BY coverage gate. Summons iff either no GROUP BY is declared (vacuous) or every bare column in
 * the projection also appears in the GROUP BY. Aggregates, literals, function-call expressions, aliased expressions
 * pass unconditionally. GROUP BY expressions that are not bare columns (e.g.
 * `.groupBy(u => Pg.dateTrunc("day", u.created))`) don't contribute names; queries relying on that form aren't
 * helped by this check and should drop to hand SQL.
 */
sealed trait GroupCoverage[Proj <: Tuple, G <: Tuple]

object GroupCoverage {

  given empty[Proj <: Tuple]: GroupCoverage[Proj, EmptyTuple] = new GroupCoverage[Proj, EmptyTuple] {}

  given nonEmpty[Proj <: Tuple, G <: NonEmptyTuple](using
    ev: AllCovered[Proj, GroupNames[G]]
  ): GroupCoverage[Proj, G] = new GroupCoverage[Proj, G] {}

}
