package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/** Window-only functions. Args of input expression(s) propagate. */
trait PgWindow {

  // ---- Ranking functions (no input) -------------------------------------------------------------

  val rowNumber:   TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("row_number()"),   skunk.codec.all.int8)
  val rank:        TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("rank()"),         skunk.codec.all.int8)
  val denseRank:   TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("dense_rank()"),   skunk.codec.all.int8)
  val percentRank: TypedExpr[Double, Void] = TypedExpr(TypedExpr.voidFragment("percent_rank()"), skunk.codec.all.float8)
  val cumeDist:    TypedExpr[Double, Void] = TypedExpr(TypedExpr.voidFragment("cume_dist()"),    skunk.codec.all.float8)

  /** `ntile(n)`. */
  inline def ntile[A](n: TypedExpr[Int, A]): TypedExpr[Int, A] = {
    val frag = TypedExpr.wrap("ntile(", n.fragment, ")")
    TypedExpr(frag, skunk.codec.all.int4)
  }

  // ---- Offset access functions ------------------------------------------------------------------

  def lag[T, A](expr: TypedExpr[T, A]): TypedExpr[Option[T], A] =
    unaryOpt("lag", expr)

  inline def lag[T, A1, A2](expr: TypedExpr[T, A1], offset: TypedExpr[Int, A2]): TypedExpr[Option[T], Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](expr.fragment, ", ", offset.fragment)
    val frag  = TypedExpr.wrap("lag(", inner, ")")
    TypedExpr(frag, expr.codec.opt)
  }

  /** `lag(expr, offset, default)`. */
  inline def lag[T, A1, A2, A3](
    expr:    TypedExpr[T, A1],
    offset:  TypedExpr[Int, A2],
    default: TypedExpr[T, A3]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "lag", List(expr.fragment, offset.fragment, default.fragment), expr.codec
    )

  def lead[T, A](expr: TypedExpr[T, A]): TypedExpr[Option[T], A] =
    unaryOpt("lead", expr)

  inline def lead[T, A1, A2](expr: TypedExpr[T, A1], offset: TypedExpr[Int, A2]): TypedExpr[Option[T], Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](expr.fragment, ", ", offset.fragment)
    val frag  = TypedExpr.wrap("lead(", inner, ")")
    TypedExpr(frag, expr.codec.opt)
  }

  /** `lead(expr, offset, default)`. */
  inline def lead[T, A1, A2, A3](
    expr:    TypedExpr[T, A1],
    offset:  TypedExpr[Int, A2],
    default: TypedExpr[T, A3]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: EmptyTuple](
      "lead", List(expr.fragment, offset.fragment, default.fragment), expr.codec
    )

  // ---- Value functions --------------------------------------------------------------------------

  def firstValue[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = {
    val frag = TypedExpr.wrap("first_value(", expr.fragment, ")")
    TypedExpr[T, A](frag, expr.codec)
  }

  def lastValue[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = {
    val frag = TypedExpr.wrap("last_value(", expr.fragment, ")")
    TypedExpr[T, A](frag, expr.codec)
  }

  inline def nthValue[T, A1, A2](expr: TypedExpr[T, A1], n: TypedExpr[Int, A2]): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSepInl[A1, A2](expr.fragment, ", ", n.fragment)
    val frag  = TypedExpr.wrap("nth_value(", inner, ")")
    TypedExpr(frag, expr.codec)
  }

  private def unaryOpt[T, A](name: String, expr: TypedExpr[T, A]): TypedExpr[Option[T], A] = {
    val frag = TypedExpr.wrap(s"$name(", expr.fragment, ")")
    TypedExpr[Option[T], A](frag, expr.codec.opt)
  }

}
