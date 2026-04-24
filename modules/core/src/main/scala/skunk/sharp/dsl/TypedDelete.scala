package skunk.sharp.dsl

import skunk.{Fragment}
import skunk.sharp.Table
import skunk.sharp.internal.{RawConstants, TypedWhere}
import skunk.util.Origin

/**
 * Typed DELETE — `DELETE FROM "t" WHERE <typed-pred>` with visible `Args` type parameter.
 *
 * Entered from `DeleteBuilder.where` with a typed lambda (`TypedWhere[A]`). `.compile` produces
 * `CompiledCommand[Args]`, which gives the user `.typedCommand: skunk.Command[Args]` and
 * `.prepared(session): F[PreparedCommand[F, Args]]`.
 */
final class TypedDeleteReady[Cols <: Tuple, Name <: String & Singleton, Args] private[dsl] (
  private[dsl] val table:          Table[Cols, Name],
  private[dsl] val whereFragment:  Fragment[Args],
  private[dsl] val whereArgs:      Args
) {

  /** Chain another typed WHERE — AND-joined, extends `Args` to `(Args, A)`. */
  def where[A](f: skunk.sharp.ColumnsView[Cols] => TypedWhere[A]): TypedDeleteReady[Cols, Name, (Args, A)] = {
    val pred = f(table.columnsView)
    val combinedParts =
      RawConstants.OPEN_PAREN.fragment.parts ++
        whereFragment.parts ++
        RawConstants.AND.fragment.parts ++
        pred.fragment.parts ++
        RawConstants.CLOSE_PAREN.fragment.parts
    val combinedEncoder = whereFragment.encoder.product(pred.fragment.encoder)
    val combinedFragment: Fragment[(Args, A)] =
      Fragment(combinedParts, combinedEncoder, Origin.unknown)
    new TypedDeleteReady[Cols, Name, (Args, A)](table, combinedFragment, (whereArgs, pred.args))
  }

  def compile: CompiledCommand[Args] = {
    val header      = table.deleteFromHeader
    val wherePrefix = RawConstants.WHERE
    val combinedParts =
      header.fragment.parts ++
        wherePrefix.fragment.parts ++
        whereFragment.parts
    val combinedFragment: Fragment[Args] =
      Fragment(combinedParts, whereFragment.encoder, Origin.unknown)
    CompiledCommand.mk[Args](combinedFragment, whereArgs)
  }

}
