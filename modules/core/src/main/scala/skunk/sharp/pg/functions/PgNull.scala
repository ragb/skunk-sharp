package skunk.sharp.pg.functions

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped

/** NULL-handling helpers. Mixed into [[skunk.sharp.Pg]]. */
trait PgNull {

  /**
   * Typed `NULL` literal — renders inline as `NULL`, codec is `.opt`'d so the decoder expects an `Option`. The type
   * parameter pins the SQL result-type shape (needed in projections, `coalesce(nullOf[String], …)`, etc.). Not named
   * `Null` to avoid colliding with Scala's `Null` type.
   *
   * `lit(null)` isn't an option because `null: Null` has no `PgTypeFor` instance — the caller has to specify the target
   * type explicitly.
   */
  def nullOf[T](using pf: PgTypeFor[T]): TypedExpr[Option[T]] =
    TypedExpr(TypedExpr.raw("NULL"), pf.codec.opt)

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
      TypedExpr.raw("nullif(") |+| a.render |+| TypedExpr.raw(", ") |+| TypedExpr.parameterised(b).render |+|
        TypedExpr.raw(")"),
      pf.codec.opt
    )

}
