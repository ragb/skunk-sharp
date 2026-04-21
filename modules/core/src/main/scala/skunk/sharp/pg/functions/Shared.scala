package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped

// ---- Shared type-level helpers ------------------------------------------------------------------

/**
 * Evidence that `T` (possibly wrapped in `Option`) is a string-like type — covers `String`, all string tag types
 * (`Varchar[N]`, `Bpchar[N]`, `Text`), and their `Option` variants. Used by the [[PgString]] family as the implicit
 * constraint so callers keep tag information through the call.
 */
type StrLike[T] = Stripped[T] <:< String

/**
 * Preserve `T`'s nullability (or lack thereof) while changing the "base" type to `U`. Used by PG functions that have a
 * fixed non-input return type but still propagate NULL — e.g. `length(null) → null`, so
 * `length(TypedExpr[Option[String]]) → TypedExpr[Option[Int]]`.
 */
type Lift[T, U] = T match {
  case Option[x] => Option[U]
  case _         => U
}

/**
 * Type-level mapping of Postgres's `sum(I)` result type. Codec resolution for the result type flows through
 * [[skunk.sharp.pg.PgTypeFor]] — no dedicated typeclass needed. Extend by adding a case here (for well-known numeric
 * types) or by supplying your own `PgTypeFor[Out]` for a custom opaque type.
 */
type SumOf[I] = I match {
  case Short | Int       => Long
  case Long | BigDecimal => BigDecimal
  case Float             => Float
  case Double            => Double
}

/** Type-level mapping of Postgres's `avg(I)` result type. See [[SumOf]] for the same rationale. */
type AvgOf[I] = I match {
  case Short | Int | Long | BigDecimal => BigDecimal
  case Float | Double                  => Double
}

// ---- Shared shape helpers — private to the functions package ------------------------------------
//
// These collapse the "unary function wrapping a codec" idiom that every Pg trait uses. Kept
// `private[functions]` so third-party traits mixing into `Pg` still see them when defined in this
// package, but they don't leak into user code.

private[functions] def sameTypeFn[T](name: String, e: TypedExpr[T]): TypedExpr[T] =
  TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

private[functions] def stringPreserveFn[T](name: String, e: TypedExpr[T])(using StrLike[T]): TypedExpr[T] =
  TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), e.codec)

private[functions] def doubleFn[T](name: String, e: TypedExpr[T])(using
  pf: PgTypeFor[Lift[T, Double]]
): TypedExpr[Lift[T, Double]] =
  TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), pf.codec)

private[functions] def stringToIntFn[T](name: String, e: TypedExpr[T])(using
  ev: StrLike[T],
  pf: PgTypeFor[Lift[T, Int]]
): TypedExpr[Lift[T, Int]] =
  TypedExpr(TypedExpr.raw(s"$name(") |+| e.render |+| TypedExpr.raw(")"), pf.codec)
