package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.{PgFunction, TypedExpr}
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
    PgFunction.call1("coalesce", a, pf.codec)
  }

  /** `coalesce(a, b)` — Args = `Concat[A1, A2]`. */
  inline def coalesce[T, A1, A2](a: TypedExpr[T, A1], b: TypedExpr[T, A2])(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.Concat[A1, A2]] = {
    PgFunction.call2("coalesce", a, b, pf.codec)
  }

  /** `coalesce(a, b, c)` — `Args` flattens to `(A1, A2, A3)` via `Where.Concat` (Void slots dropped). */
  inline def coalesce[T, A1, A2, A3](a: TypedExpr[T, A1], b: TypedExpr[T, A2], c: TypedExpr[T, A3])(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]] = {
    val projector: Where.Concat[Where.Concat[A1, A2], A3] => List[Any] =
      Where.foldOf[Where.Concat[Where.Concat[A1, A2], A3]](Where.slotCodes[(A1, A2, A3)])
    val combined = TypedExpr.combineList[Where.Concat[Where.Concat[A1, A2], A3]](
      List(a.fragment, b.fragment, c.fragment),
      ", ",
      projector
    )
    val frag = TypedExpr.wrap("coalesce(", combined, ")")
    TypedExpr[T, Where.Concat[Where.Concat[A1, A2], A3]](frag, pf.codec)
  }

  /** `coalesce(a, b, c, d)` — `Args` flattens to the non-Void slots of `(A1, A2, A3, A4)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment),
      pf.codec
    )

  /** `coalesce(a, b, c, d, e)` — `Args` flattens to the non-Void slots of `(A1…A5)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4, A5](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4],
    e: TypedExpr[T, A5]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment),
      pf.codec
    )

  /** `coalesce(a, b, c, d, e, f)` — `Args` flattens to the non-Void slots of `(A1…A6)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4, A5, A6](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4],
    e: TypedExpr[T, A5],
    f: TypedExpr[T, A6]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment),
      pf.codec
    )

  /** `coalesce(a, b, c, d, e, f, g)` — `Args` flattens to the non-Void slots of `(A1…A7)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4, A5, A6, A7](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4],
    e: TypedExpr[T, A5],
    f: TypedExpr[T, A6],
    g: TypedExpr[T, A7]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment),
      pf.codec
    )

  /** `coalesce(a, b, c, d, e, f, g, h)` — `Args` flattens to the non-Void slots of `(A1…A8)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4, A5, A6, A7, A8](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4],
    e: TypedExpr[T, A5],
    f: TypedExpr[T, A6],
    g: TypedExpr[T, A7],
    h: TypedExpr[T, A8]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment),
      pf.codec
    )

  /** `coalesce(a, b, c, d, e, f, g, h, i)` — `Args` flattens to the non-Void slots of `(A1…A9)` via `Where.Concat`. */
  inline def coalesce[T, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    a: TypedExpr[T, A1],
    b: TypedExpr[T, A2],
    c: TypedExpr[T, A3],
    d: TypedExpr[T, A4],
    e: TypedExpr[T, A5],
    f: TypedExpr[T, A6],
    g: TypedExpr[T, A7],
    h: TypedExpr[T, A8],
    i: TypedExpr[T, A9]
  )(using
    pf: PgTypeFor[T]
  ): TypedExpr[T, Where.FoldConcat[A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple]] =
    PgFunction.naryTypedFold[T, A1 *: A2 *: A3 *: A4 *: A5 *: A6 *: A7 *: A8 *: A9 *: EmptyTuple](
      "coalesce",
      List(a.fragment, b.fragment, c.fragment, d.fragment, e.fragment, f.fragment, g.fragment, h.fragment, i.fragment),
      pf.codec
    )

  /** `nullif(a, b)` — returns NULL if `a = b`, else `a`. */
  inline def nullif[T, A1, A2](
    a: TypedExpr[T, A1],
    b: TypedExpr[Stripped[T], A2]
  )(using pf: PgTypeFor[Stripped[T]]): TypedExpr[Option[Stripped[T]], Where.Concat[A1, A2]] = {
    PgFunction.call2("nullif", a, b, pf.codec.opt)
  }

}
