package skunk.sharp

import skunk.Codec

/**
 * A typed reference to a column in the current WHERE/SELECT lambda.
 *
 * `T` is the Scala value type; `Null` tracks nullability at the type level so operators like `isNull` can be offered
 * only on nullable columns. `TypedColumn` is a [[TypedExpr]] leaf — operators produce new `TypedExpr`s.
 */
final class TypedColumn[T, Null <: Boolean](
  val name: String,
  val codec: Codec[T],
  val qualifier: Option[String] = None,
  val quoteQualifier: Boolean = true
) extends TypedExpr[T] {

  /** SQL identifier form — `"name"` or `"alias"."name"` / `alias."name"` depending on qualifier state. */
  def sqlRef: String =
    qualifier match {
      case None                      => s""""$name""""
      case Some(q) if quoteQualifier => s""""$q"."$name""""
      case Some(q) /* unquoted */    => s"""$q."$name""""
    }

  def render: skunk.AppliedFragment = TypedExpr.raw(sqlRef)

}

object TypedColumn {

  /**
   * Build a `TypedColumn` from a [[Column]] descriptor, erasing the singleton-name type parameter — by the time the
   * WHERE/SELECT lambda runs, the name's singleton role is over.
   */
  def of[T, N <: String & Singleton, Null <: Boolean, D <: Boolean](c: Column[T, N, Null, D]): TypedColumn[T, Null] =
    new TypedColumn[T, Null](c.name, c.codec)

  /**
   * Build a `TypedColumn` whose rendering is prefixed with a table/alias qualifier (`"u"."col"`). By default the
   * qualifier is double-quoted — safe for arbitrary user-provided aliases.
   */
  def qualified[T, N <: String & Singleton, Null <: Boolean, D <: Boolean](
    c: Column[T, N, Null, D],
    qualifier: String
  ): TypedColumn[T, Null] =
    new TypedColumn[T, Null](c.name, c.codec, Some(qualifier), quoteQualifier = true)

  /**
   * Same as [[qualified]] but leaves the qualifier unquoted in the SQL (`excluded."col"` instead of
   * `"excluded"."col"`). Needed for Postgres pseudo-tables like `excluded` — a quoted `"excluded"` would be treated as
   * a user identifier and fail to reference the incoming-row pseudo-table in `ON CONFLICT DO UPDATE`.
   */
  def qualifiedRaw[T, N <: String & Singleton, Null <: Boolean, D <: Boolean](
    c: Column[T, N, Null, D],
    qualifier: String
  ): TypedColumn[T, Null] =
    new TypedColumn[T, Null](c.name, c.codec, Some(qualifier), quoteQualifier = false)

}
