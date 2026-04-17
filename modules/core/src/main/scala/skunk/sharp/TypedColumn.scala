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
  val qualifier: Option[String] = None
) extends TypedExpr[T] {

  def render: skunk.AppliedFragment =
    TypedExpr.raw(qualifier.fold(s""""$name"""")(q => s"""$q."$name""""))

}

object TypedColumn {

  /**
   * Build a `TypedColumn` from a [[Column]] descriptor, erasing the singleton-name type parameter — by the time the
   * WHERE/SELECT lambda runs, the name's singleton role is over.
   */
  def of[T, N <: String & Singleton, Null <: Boolean, D <: Boolean](c: Column[T, N, Null, D]): TypedColumn[T, Null] =
    new TypedColumn[T, Null](c.name, c.codec)

  /**
   * Build a `TypedColumn` whose rendering is prefixed with a table/alias qualifier (`excluded.<col>`, `u.<col>`, …).
   */
  def qualified[T, N <: String & Singleton, Null <: Boolean, D <: Boolean](
    c: Column[T, N, Null, D],
    qualifier: String
  ): TypedColumn[T, Null] =
    new TypedColumn[T, Null](c.name, c.codec, Some(qualifier))

}
