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
