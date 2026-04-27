package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.pg.PgTypeFor

/** Window-only functions. Args of input expression(s) propagate. */
trait PgWindow {

  // ---- Ranking functions (no input) -------------------------------------------------------------

  val rowNumber:   TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("row_number()"),   skunk.codec.all.int8)
  val rank:        TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("rank()"),         skunk.codec.all.int8)
  val denseRank:   TypedExpr[Long, Void]   = TypedExpr(TypedExpr.voidFragment("dense_rank()"),   skunk.codec.all.int8)
  val percentRank: TypedExpr[Double, Void] = TypedExpr(TypedExpr.voidFragment("percent_rank()"), skunk.codec.all.float8)
  val cumeDist:    TypedExpr[Double, Void] = TypedExpr(TypedExpr.voidFragment("cume_dist()"),    skunk.codec.all.float8)

  /** `ntile(n)` — `n` is a literal Int rendered inline. */
  def ntile(n: Int): TypedExpr[Int, Void] =
    TypedExpr(TypedExpr.voidFragment(s"ntile($n)"), skunk.codec.all.int4)

  // ---- Offset access functions ------------------------------------------------------------------

  def lag[T, A](expr: TypedExpr[T, A]): TypedExpr[Option[T], A] =
    unaryOpt("lag", expr)

  def lag[T, A](expr: TypedExpr[T, A], offset: Int): TypedExpr[Option[T], A] = {
    val parts = e2parts("lag(", expr.fragment, s", $offset)")
    val frag  = Fragment[A](parts, expr.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[Option[T], A](frag, expr.codec.opt)
  }

  def lag[T](expr: TypedExpr[T, ?], offset: Int, default: T)(using pf: PgTypeFor[T]): TypedExpr[T, Void] = {
    // expr's encoder + Param.bind(default)'s encoder must combine via joinedVoid so the default's bound
    // value flows through. Previously the encoder was just the column's Void.codec — Postgres saw `$1` in
    // the SQL but no encoder slot for it.
    val defFrag = Param.bind[T](default).fragment
    val mid     = TypedExpr.joinedVoid(s", $offset, ", List(expr.fragment, defFrag))
    val frag    = TypedExpr.wrap("lag(", mid, ")")
    TypedExpr[T, Void](frag, expr.codec)
  }

  def lead[T, A](expr: TypedExpr[T, A]): TypedExpr[Option[T], A] =
    unaryOpt("lead", expr)

  def lead[T, A](expr: TypedExpr[T, A], offset: Int): TypedExpr[Option[T], A] = {
    val parts = e2parts("lead(", expr.fragment, s", $offset)")
    val frag  = Fragment[A](parts, expr.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[Option[T], A](frag, expr.codec.opt)
  }

  def lead[T](expr: TypedExpr[T, ?], offset: Int, default: T)(using pf: PgTypeFor[T]): TypedExpr[T, Void] = {
    val defFrag = Param.bind[T](default).fragment
    val mid     = TypedExpr.joinedVoid(s", $offset, ", List(expr.fragment, defFrag))
    val frag    = TypedExpr.wrap("lead(", mid, ")")
    TypedExpr[T, Void](frag, expr.codec)
  }

  // ---- Value functions --------------------------------------------------------------------------

  def firstValue[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = {
    val frag = TypedExpr.wrap("first_value(", expr.fragment, ")")
    TypedExpr[T, A](frag, expr.codec)
  }

  def lastValue[T, A](expr: TypedExpr[T, A]): TypedExpr[T, A] = {
    val frag = TypedExpr.wrap("last_value(", expr.fragment, ")")
    TypedExpr[T, A](frag, expr.codec)
  }

  def nthValue[T, A](expr: TypedExpr[T, A], n: Int): TypedExpr[T, A] = {
    val parts = e2parts("nth_value(", expr.fragment, s", $n)")
    val frag  = Fragment[A](parts, expr.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[T, A](frag, expr.codec)
  }

  private def unaryOpt[T, A](name: String, expr: TypedExpr[T, A]): TypedExpr[Option[T], A] = {
    val frag = TypedExpr.wrap(s"$name(", expr.fragment, ")")
    TypedExpr[Option[T], A](frag, expr.codec.opt)
  }

  private def e2parts(prefix: String, f: Fragment[?], suffix: String): List[Either[String, cats.data.State[Int, String]]] =
    List[Either[String, cats.data.State[Int, String]]](Left(prefix)) ++ f.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(suffix))

}
