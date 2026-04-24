package skunk.sharp.dsl

import skunk.{Codec, Fragment}
import skunk.sharp.{Column, Table, TypedExpr}
import skunk.sharp.internal.{tupleCodec, RawConstants, CompileChecks}
import skunk.util.Origin

import scala.NamedTuple
import scala.compiletime.constValueTuple

/**
 * Typed INSERT — `INSERT INTO "t" (col1, col2) VALUES ($1, $2)` with `Args` carrying the captured row's value
 * tuple. `users.insert.insertT(row).compile` yields `CompiledCommand[Args]` where `Args` is the row's
 * `DropNames[R]` — exactly the tuple of values the user passed, in order.
 *
 * Single-row form for now; batch (`Reducible`) and `INSERT ... SELECT` typed paths can follow the same shape.
 */
object TypedInsertCommand {
  def mk[Cols <: Tuple, Args](
    table:     Table[Cols, ?],
    projected: List[Column[?, ?, ?, ?]],
    args:      Args
  ): TypedInsertCommand[Cols, Args] = new TypedInsertCommand[Cols, Args](table, projected, args)
}

final class TypedInsertCommand[Cols <: Tuple, Args] private[dsl] (
  private[dsl] val table:     Table[Cols, ?],
  private[dsl] val projected: List[Column[?, ?, ?, ?]],
  private[dsl] val args:      Args
) {

  def compile: CompiledCommand[Args] = {
    val projectionsList = projected.map(c => s""""${c.name}"""").mkString(", ")
    val header          = TypedExpr.raw(s"INSERT INTO ${table.qualifiedName} ($projectionsList) ")
    val perRow: Codec[Tuple] = tupleCodec(projected.map(_.codec))
    val rowEnc       = perRow.values
    val combinedParts =
      header.fragment.parts ++
        RawConstants.VALUES.fragment.parts ++
        List(Right(rowEnc.sql))
    val frag: Fragment[Args] =
      Fragment(combinedParts, rowEnc.asInstanceOf[skunk.Encoder[Args]], Origin.unknown)
    CompiledCommand.mk[Args](frag, args)
  }

}

extension [Cols <: Tuple](b: InsertBuilder[Cols])
  /**
   * Typed INSERT — same compile-time checks as the untyped `.apply` (names exist, all required columns
   * present, value types match), but returns a `TypedInsertCommand[Cols, DropNames[R]]` whose `Args` is the
   * tuple of value types in the order the user listed them in the row.
   */
  inline def insertT[R <: NamedTuple.AnyNamedTuple](row: R): TypedInsertCommand[Cols, NamedTuple.DropNames[R]] = {
    CompileChecks.requireAllNamesInCols[Cols, NamedTuple.Names[R]]
    CompileChecks.requireCoversRequired[Cols, NamedTuple.Names[R]]
    CompileChecks.requireValueTypesMatch[Cols, NamedTuple.Names[R], NamedTuple.DropNames[R]]
    val names      = constValueTuple[NamedTuple.Names[R]].toList.asInstanceOf[List[String]]
    val valuesList = row.asInstanceOf[Tuple].toList
    val cols       = b.table.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    // Project to the columns the user named, in the user's order.
    val projected  = names.map(n => cols.find(_.name == n).get)
    val argsTuple  = Tuple.fromArray(valuesList.toArray[Any]).asInstanceOf[NamedTuple.DropNames[R]]
    TypedInsertCommand.mk[Cols, NamedTuple.DropNames[R]](b.table, projected, argsTuple)
  }
