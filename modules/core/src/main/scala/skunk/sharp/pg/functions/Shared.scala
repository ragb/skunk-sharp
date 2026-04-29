package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped
import skunk.sharp.where.Where

// ---- Shared type-level helpers ------------------------------------------------------------------

type StrLike[T] = Stripped[T] <:< String

type Lift[T, U] = T match {
  case Option[x] => Option[U]
  case _         => U
}

type SumOf[I] = I match {
  case Short | Int       => Long
  case Long | BigDecimal => BigDecimal
  case Float             => Float
  case Double            => Double
}

type AvgOf[I] = I match {
  case Short | Int | Long | BigDecimal => BigDecimal
  case Float | Double                  => Double
}

// ---- Shared shape helpers — Args-threading ------------------------------------------------------

private[functions] def sameTypeFn[T, A](name: String, e: TypedExpr[T, A]): TypedExpr[T, A] = {
  val frag = TypedExpr.wrap(s"$name(", e.fragment, ")")
  TypedExpr[T, A](frag, e.codec)
}

private[functions] def stringPreserveFn[T, A](name: String, e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] =
  sameTypeFn(name, e)

private[functions] def doubleFn[T, A](name: String, e: TypedExpr[T, A])(using
  pf: PgTypeFor[Lift[T, Double]]
): TypedExpr[Lift[T, Double], A] = {
  val frag = TypedExpr.wrap(s"$name(", e.fragment, ")")
  TypedExpr[Lift[T, Double], A](frag, pf.codec)
}

private[functions] def stringToIntFn[T, A](name: String, e: TypedExpr[T, A])(using
  ev: StrLike[T],
  pf: PgTypeFor[Lift[T, Int]]
): TypedExpr[Lift[T, Int], A] = {
  val frag = TypedExpr.wrap(s"$name(", e.fragment, ")")
  TypedExpr[Lift[T, Int], A](frag, pf.codec)
}

private[functions] def twoArgDoubleFn[Y, X, AY, AX](
  name: String,
  y:    TypedExpr[Y, AY],
  x:    TypedExpr[X, AX]
): TypedExpr[Double, Where.Concat[AY, AX]] = {
  val inner = TypedExpr.combineSep(y.fragment, ", ", x.fragment)
  val frag  = TypedExpr.wrap(s"$name(", inner, ")")
  TypedExpr[Double, Where.Concat[AY, AX]](frag, skunk.codec.all.float8)
}
