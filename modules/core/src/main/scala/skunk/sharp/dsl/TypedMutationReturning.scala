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

/**
 * Extension to map a `CompiledQuery[Args, R]` whose `R` is a product (named tuple from `.returningAll` or a
 * plain tuple from `.returningTuple`) to a case class via `Mirror.ProductOf`. The mapping is codec-level —
 * same `Args`, new codec that reads into `T`.
 */
extension [Args, R](q: CompiledQuery[Args, R])
  /** Map the row shape to a case class `T` whose field types align with `R` (named-tuple- or tuple-wise). */
  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] {
      type MirroredElemTypes = skunk.sharp.dsl.MutationReturning.Unwrap[R] & Tuple
    }
  ): CompiledQuery[Args, T] = {
    val mapped: skunk.Codec[T] = q.codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[R]
    )
    CompiledQuery.mk[Args, T](q.fragment, q.args, mapped)
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

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedDeleteReady[Cols, Name, Args])
  /** Tuple `RETURNING <e1>, <e2>, …` — returns a `CompiledQuery[Args, ExprOutputs[T]]`. */
  def returningTuple[T <: NonEmptyTuple](
    f: skunk.sharp.ColumnsView[Cols] => T
  ): CompiledQuery[Args, ExprOutputs[T]] = {
    val exprs = f(b.table.columnsView).toList.asInstanceOf[List[TypedExpr[?]]]
    val base  = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      exprs,
      TypedReturning.tupleExprCodec[ExprOutputs[T]](exprs)
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

extension [Cols <: Tuple, Name <: String & Singleton, Args](b: TypedUpdateReady[Cols, Name, Args])
  /** Tuple `RETURNING <e1>, <e2>, …`. */
  def returningTuple[T <: NonEmptyTuple](
    f: skunk.sharp.ColumnsView[Cols] => T
  ): CompiledQuery[Args, ExprOutputs[T]] = {
    val exprs = f(b.table.columnsView).toList.asInstanceOf[List[TypedExpr[?]]]
    val base  = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      exprs,
      TypedReturning.tupleExprCodec[ExprOutputs[T]](exprs)
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

extension [Cols <: Tuple, Args](b: TypedInsertCommand[Cols, Args])
  /** Tuple `RETURNING <e1>, <e2>, …`. */
  def returningTuple[T <: NonEmptyTuple](
    f: skunk.sharp.ColumnsView[Cols] => T
  ): CompiledQuery[Args, ExprOutputs[T]] = {
    val exprs = f(b.table.columnsView).toList.asInstanceOf[List[TypedExpr[?]]]
    val base  = b.compile
    TypedReturning.compile(
      base.fragment,
      base.args,
      exprs,
      TypedReturning.tupleExprCodec[ExprOutputs[T]](exprs)
    )
  }
