package skunk.sharp.contrib.ltree

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/**
 * `ltree` function helpers. Mix into your own `Pg` bundle or call via the [[PgLtree]] namespace. Every argument is a
 * typed expression so `Args` threads naturally; rendered SQL is `<name>(<args>)`.
 */
trait PgLtree {

  /** Longest common ancestor of two `ltree` values. */
  inline def lca[A, B](a: TypedExpr[LTree, A], b: TypedExpr[LTree, B]): TypedExpr[LTree, Where.Concat[A, B]] =
    PgFunction.binary[LTree, LTree, LTree, A, B]("lca")(a, b)

  /** Number of labels in the path. */
  inline def nlevel[A](e: TypedExpr[LTree, A]): TypedExpr[Int, A] =
    PgFunction.unary[LTree, Int, A]("nlevel")(e)

  /** Sub-path from `start` (0-based, inclusive) of `len` labels. */
  inline def subltree[A, B, C](
    e: TypedExpr[LTree, A],
    start: TypedExpr[Int, B],
    len: TypedExpr[Int, C]
  ): TypedExpr[LTree, Where.Concat[A, Where.Concat[B, C]]] = {
    val mid   = TypedExpr.combineSepInl[B, C](start.fragment, ", ", len.fragment)
    val inner = TypedExpr.combineSepInl[A, Where.Concat[B, C]](e.fragment, ", ", mid)
    val frag  = TypedExpr.wrap("subltree(", inner, ")")
    TypedExpr[LTree, Where.Concat[A, Where.Concat[B, C]]](frag, LTree.codec)
  }

  /** Sub-path starting at `offset`. Negative `offset` counts from the end. */
  inline def subpath[A, B](
    e: TypedExpr[LTree, A],
    offset: TypedExpr[Int, B]
  ): TypedExpr[LTree, Where.Concat[A, B]] =
    PgFunction.binary[LTree, Int, LTree, A, B]("subpath")(e, offset)

  /** Position of the first occurrence of `b` within `a` (0-based, -1 if absent). */
  inline def index[A, B](
    a: TypedExpr[LTree, A],
    b: TypedExpr[LTree, B]
  ): TypedExpr[Int, Where.Concat[A, B]] =
    PgFunction.binary[LTree, LTree, Int, A, B]("index")(a, b)

  /** Text → `ltree`. Rare; prefer just declaring the column as `LTree` from the start. */
  inline def text2ltree[A](e: TypedExpr[String, A]): TypedExpr[LTree, A] =
    PgFunction.unary[String, LTree, A]("text2ltree")(e)

  /** `ltree` → text. */
  inline def ltree2text[A](e: TypedExpr[LTree, A]): TypedExpr[String, A] =
    PgFunction.unary[LTree, String, A]("ltree2text")(e)

}

object PgLtree extends PgLtree
