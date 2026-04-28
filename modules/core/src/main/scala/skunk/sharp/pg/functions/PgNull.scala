package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{Param, PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped

/** NULL-handling helpers. Mixed into [[skunk.sharp.Pg]]. */
trait PgNull {

  /** Typed `NULL` literal — renders inline as `NULL`, Args = Void. */
  def nullOf[T](using pf: PgTypeFor[T]): TypedExpr[Option[T], Void] =
    TypedExpr(TypedExpr.voidFragment("NULL"), pf.codec.opt)

  /** `coalesce(a, b, c, …)` — first non-null argument. Args = `Void` (variadic typed-Args is roadmap). */
  def coalesce[T](args: TypedExpr[T, ?]*)(using pfr: PgTypeFor[T]): TypedExpr[T, Void] =
    PgFunction.nary[T]("coalesce", args*).asInstanceOf[TypedExpr[T, Void]]

  /**
   * `nullif(a, b)` — returns NULL if `a = b`, else `a`. `b` is a runtime value baked via [[Param.bind]];
   * Args of the result equals Args of `a`.
   */
  def nullif[T, A](a: TypedExpr[T, A], b: Stripped[T])(using
    pf: PgTypeFor[Stripped[T]]
  ): TypedExpr[Option[Stripped[T]], A] = {
    val bFrag = Param.bind[Stripped[T]](b)(using pf).fragment
    val inner = TypedExpr.combineSep(a.fragment, ", ", bFrag).asInstanceOf[Fragment[A]]
    val frag  = TypedExpr.wrap("nullif(", inner, ")")
    TypedExpr[Option[Stripped[T]], A](frag, pf.codec.opt)
  }

}
