package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Stripped

/** NULL-handling helpers. Mixed into [[skunk.sharp.Pg]]. */
trait PgNull {

  /** `coalesce(a, b, c, …)` — first non-null argument. */
  def coalesce[T](args: TypedExpr[T]*)(using pfr: PgTypeFor[T]): TypedExpr[T] =
    PgFunction.nary[T]("coalesce", args*)

  /**
   * `nullif(a, b)` — returns NULL if `a = b`, else `a`. Result is always nullable because the caller can't prove
   * `a ≠ b` statically. `b` must have the same underlying type as `a` (modulo `Option` wrapping).
   */
  def nullif[T](a: TypedExpr[T], b: Stripped[T])(using
    pf: PgTypeFor[Stripped[T]]
  ): TypedExpr[Option[Stripped[T]]] =
    TypedExpr(
      TypedExpr.raw("nullif(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.lit(b).render |+| TypedExpr.raw(")"),
      pf.codec.opt
    )

}
