package skunk.sharp.dsl

import skunk.{Codec, Fragment}
import skunk.sharp.{Column, NamedRowOf, Table, TypedColumn, TypedExpr}
import skunk.sharp.internal.{tupleCodec, RawConstants}
import skunk.util.Origin


/**
 * Helper — append `RETURNING <exprs>` to a typed command's fragment and produce a `CompiledQuery[Args, R]`
 * with the same `Args` (RETURNING doesn't introduce parameters for column-only / aggregate projections).
 */
private[dsl] object TypedReturning {

  def compile[Args, R](
    baseFragment:  Fragment[Args],
    baseArgs:      Args,
    returningList: List[TypedExpr[?]],
    returnCodec:   Codec[R]
  ): CompiledQuery[Args, R] = {
    val listAf = TypedExpr.joined(returningList.map(_.render), ", ")
    val parts =
      baseFragment.parts ++
        RawConstants.RETURNING.fragment.parts ++
        listAf.fragment.parts
    val combinedFragment: Fragment[Args] = Fragment(parts, baseFragment.encoder, Origin.unknown)
    CompiledQuery.mk[Args, R](combinedFragment, baseArgs, returnCodec)
  }

  /** Build the `RETURNING *` projection list for a whole-row return — every column of the target table. */
  def wholeRowExprs[Cols <: Tuple](table: Table[Cols, ?]): List[TypedExpr[?]] =
    table.columns
      .toList
      .asInstanceOf[List[Column[?, ?, ?, ?]]]
      .map(c => TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Tuple]]))

  def wholeRowCodec[Cols <: Tuple](table: Table[Cols, ?]): Codec[NamedRowOf[Cols]] =
    skunk.sharp.internal.rowCodec(table.columns).asInstanceOf[Codec[NamedRowOf[Cols]]]

  /** Build a tuple-returning codec from a heterogeneous list of `TypedExpr`s. */
  def tupleExprCodec[T](exprs: List[TypedExpr[?]]): Codec[T] =
    tupleCodec(exprs.map(_.codec)).asInstanceOf[Codec[T]]

}

// ---- TypedDeleteReady RETURNING -----------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedDeleteReady[Cols, Name, Args])
  /** Single-expression `RETURNING`. */
  def returning[T](f: skunk.sharp.ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val expr = f(b.table.columnsView)
    val base = b.compile
    TypedReturning.compile(base.fragment, base.args, List(expr), expr.codec)
  }

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedDeleteReady[Cols, Name, Args])
  /** Whole-row `RETURNING *`. */
  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val base = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      TypedReturning.wholeRowExprs(b.table),
      TypedReturning.wholeRowCodec(b.table)
    )
  }

// ---- TypedUpdateReady RETURNING -----------------------------------------------------------------

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedUpdateReady[Cols, Name, Args])
  /** Single-expression `RETURNING`. */
  def returning[T](f: skunk.sharp.ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val expr = f(b.table.columnsView)
    val base = b.compile
    TypedReturning.compile(base.fragment, base.args, List(expr), expr.codec)
  }

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedUpdateReady[Cols, Name, Args])
  /** Whole-row `RETURNING *`. */
  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val base = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      TypedReturning.wholeRowExprs(b.table),
      TypedReturning.wholeRowCodec(b.table)
    )
  }

// ---- TypedInsertCommand RETURNING -----------------------------------------------------------------

extension [Cols <: Tuple, Args](b: TypedInsertCommand[Cols, Args])
  /** Single-expression `RETURNING`. */
  def returning[T](f: skunk.sharp.ColumnsView[Cols] => TypedExpr[T]): CompiledQuery[Args, T] = {
    val expr = f(b.table.columnsView)
    val base = b.compile
    TypedReturning.compile(base.fragment, base.args, List(expr), expr.codec)
  }

extension [Cols <: Tuple, Args](b: TypedInsertCommand[Cols, Args])
  /** Whole-row `RETURNING *`. */
  def returningAll: CompiledQuery[Args, NamedRowOf[Cols]] = {
    val base = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      TypedReturning.wholeRowExprs(b.table),
      TypedReturning.wholeRowCodec(b.table)
    )
  }
