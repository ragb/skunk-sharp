package skunk.sharp.pg.functions

import skunk.{Fragment, Void}
import skunk.sharp.{Param, PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.ops.Stripped
import skunk.sharp.where.Where

/** NULL-handling helpers. Mixed into [[skunk.sharp.Pg]]. */
trait PgNull {

  /** Typed `NULL` literal — renders inline as `NULL`, Args = Void. */
  def nullOf[T](using pf: PgTypeFor[T]): TypedExpr[Option[T], Void] =
    TypedExpr(TypedExpr.voidFragment("NULL"), pf.codec.opt)

  /** `coalesce(a)` — single arg; Args propagates from `a`. */
  def coalesce[T, A1](a: TypedExpr[T, A1])(using pf: PgTypeFor[T]): TypedExpr[T, A1] = {
    val frag = TypedExpr.wrap("coalesce(", a.fragment, ")")
    TypedExpr[T, A1](frag, pf.codec)
  }

  /** `coalesce(a, b)` — Args = `Concat[A1, A2]`. */
  def coalesce[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2])(using
    pf: PgTypeFor[T],
    c2: Where.Concat2[A1, A2]
  ): TypedExpr[T, Where.Concat[A1, A2]] = {
    val inner = TypedExpr.combineSep(a.fragment, ", ", b.fragment)
    val frag  = TypedExpr.wrap("coalesce(", inner, ")")
    TypedExpr[T, Where.Concat[A1, A2]](frag, pf.codec)
  }

  /** `coalesce(a, b, c)` — Args = `Concat[Concat[A1, A2], A3]` (left-fold). */
  def coalesce[T, A1, A2, A3](a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3])(using
    pf:   PgTypeFor[T],
    c12:  Where.Concat2[A1, A2],
    c123: Where.Concat2[Where.Concat[A1, A2], A3]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] = combined => {
      val (a12, a3v) = c123.project(combined)
      val (a1v, a2v) = c12.project(a12.asInstanceOf[Where.Concat[A1, A2]])
      List(a1v, a2v, a3v)
    }
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment),
      ", ",
      projector
    )
    val frag = TypedExpr.wrap("coalesce(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, pf.codec)
  }

  /** `coalesce(a, b, c, d, …)` — variadic fallback for arity > 3; Args = `Void`. */
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
