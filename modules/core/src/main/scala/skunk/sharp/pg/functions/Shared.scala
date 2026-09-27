package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped

// ---- Shared type-level helpers ------------------------------------------------------------------

type StrLike[T] = Stripped[T] <:< String

type UuidLike[T] = Stripped[T] <:< java.util.UUID

type Lift[T, U] = skunk.sharp.ops.Lift[T, U]

type Lift2[L, R, U] = skunk.sharp.ops.Lift2[L, R, U]

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

private[functions] def sameTypeFn[T, A](name: String, e: TypedExpr[T, A]): TypedExpr[T, A] =
  PgFunction.call1(name, e, e.codec)

private[functions] def stringPreserveFn[T, A](name: String, e: TypedExpr[T, A])(using StrLike[T]): TypedExpr[T, A] =
  sameTypeFn(name, e)

private[functions] def doubleFn[T, A](name: String, e: TypedExpr[T, A])(using
  pf: PgTypeFor[Lift[T, Double]]
): TypedExpr[Lift[T, Double], A] =
  PgFunction.call1(name, e, pf.codec)

private[functions] def stringToIntFn[T, A](name: String, e: TypedExpr[T, A])(using
  ev: StrLike[T],
  pf: PgTypeFor[Lift[T, Int]]
): TypedExpr[Lift[T, Int], A] =
  PgFunction.call1(name, e, pf.codec)
