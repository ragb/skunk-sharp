package skunk.sharp

import skunk.Void

/**
 * How a generated column (`.withGenerated`) appears inside SET lambdas — UPDATE `.set`, `ON CONFLICT DO UPDATE`. It is
 * still a readable `TypedExpr[T, Void]`, so it can appear on the right-hand side of an assignment, but it is not a
 * [[TypedColumn]], so the regular `:=` doesn't apply; the `:=` shipped for it in `skunk.sharp.dsl` fails compilation
 * with an explanation instead. At runtime it is the plain `TypedColumn`, so rendering is unchanged.
 */
opaque type GeneratedColumn[T, Null <: Boolean, N <: String & Singleton] <: TypedExpr[T, Void] = TypedColumn[T, Null, N]

/** A SET-lambda column: [[GeneratedColumn]] for generated columns, [[TypedColumn]] for everything else. */
type SetColumnOf[T, Null <: Boolean, N <: String & Singleton, Attrs <: Tuple] =
  Contains[ColumnAttr.Generated, Attrs] match {
    case true  => GeneratedColumn[T, Null, N]
    case false => TypedColumn[T, Null, N]
  }

type SetColumnsOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, attrs] *: tail => SetColumnOf[t, nu, n, attrs] *: SetColumnsOf[tail]
  case EmptyTuple                      => EmptyTuple
}

/**
 * The view SET lambdas receive: same names and runtime values as [[ColumnsView]], but generated columns are typed as
 * [[GeneratedColumn]] so assigning to them is a compile error.
 */
type SetView[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], SetColumnsOf[Cols]]

/**
 * How a MERGE *source* column appears in a `whenMatched` SET lambda: readable (`r.stock.qty := r.incoming.qty`), but
 * not a [[TypedColumn]], so it can't be assigned — only target columns can. The `:=` shipped for it in
 * `skunk.sharp.dsl` fails compilation with an explanation. At runtime it is the plain `TypedColumn`.
 */
opaque type SourceColumn[T, Null <: Boolean, N <: String & Singleton] <: TypedExpr[T, Void] = TypedColumn[T, Null, N]

type SourceColumnsOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, attrs] *: tail => SourceColumn[t, nu, n] *: SourceColumnsOf[tail]
  case EmptyTuple                      => EmptyTuple
}

/** A relation's columns as read-only [[SourceColumn]]s — the MERGE source side of a SET lambda. */
type SourceView[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], SourceColumnsOf[Cols]]
