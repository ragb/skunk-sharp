package skunk.sharp

import skunk.{Codec, Fragment, Void}
import skunk.util.Origin

/**
 * A typed reference to a column in the current WHERE/SELECT lambda.
 *
 *   - `T` — the Scala value type.
 *   - `Null` — nullability tracked at the type level so operators like `isNull` can be offered only on nullable cols.
 *   - `N` — the column's name as a singleton string.
 *
 * `TypedColumn` is a `TypedExpr[T, Void]` leaf — a column reference contributes no parameters at execute time.
 */
final class TypedColumn[T, Null <: Boolean, N <: String & Singleton](
  val name: N,
  val codec: Codec[T],
  val qualifier: Option[String] = None,
  val quoteQualifier: Boolean = true,
  val nullable: Boolean = false
) extends TypedExpr[T, Void] {

  /** SQL identifier form — `"name"` or `"alias"."name"` / `alias."name"` depending on qualifier state. */
  lazy val sqlRef: String =
    qualifier match {
      case None                      => s""""$name""""
      case Some(q) if quoteQualifier => s""""$q"."$name""""
      case Some(q) /* unquoted */    => s"""$q."$name""""
    }

  /**
   * Cached `Fragment[Void]` for this column reference. Reused on every operator / function / projection that
   * references this column — TypedColumn instances live across builders (cached on the relation's `columnsView`)
   * so this lazy initializes once.
   */
  lazy val fragment: Fragment[Void] = Fragment(List(Left(sqlRef)), Void.codec, Origin.unknown)

}

object TypedColumn {

  /** Build a `TypedColumn` from a [[Column]] descriptor. */
  def of[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs]
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec, nullable = c.isNullable)

  /** Build a `TypedColumn` whose rendering is prefixed with a table/alias qualifier (`"u"."col"`). */
  def qualified[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs],
    qualifier: String
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec, Some(qualifier), quoteQualifier = true, nullable = c.isNullable)

  /** Same as [[qualified]] but leaves the qualifier unquoted in the SQL (`excluded."col"`). */
  def qualifiedRaw[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple](
    c: Column[T, N, Null, Attrs],
    qualifier: String
  ): TypedColumn[T, Null, N] =
    new TypedColumn[T, Null, N](c.name, c.codec, Some(qualifier), quoteQualifier = false, nullable = c.isNullable)

}
