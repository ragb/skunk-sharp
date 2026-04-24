package skunk.sharp.dsl

import skunk.Fragment
import skunk.sharp.{Table, TypedColumn}
import skunk.sharp.internal.{RawConstants, TypedWhere}
import skunk.sharp.pg.PgTypeFor
import skunk.util.Origin

/**
 * Typed SET assignment for UPDATE — `column = $1` where the captured value's type is visible as `Args`.
 *
 * Produced by the typed `:=` overload on `TypedColumn` when the RHS is a runtime literal — the value is
 * encoded as a bound parameter. Distinct from [[SetAssignment]] (the existential `Where`-style assignment) so
 * `.set` overload resolution picks the typed branch when its lambda yields a typed assignment.
 */
final class TypedSetAssignment[T, Args] private[dsl] (
  private[dsl] val col:      TypedColumn[T, ?, ?],
  private[dsl] val fragment: Fragment[Args],
  private[dsl] val args:     Args
)

object TypedSetAssignment {

  /** Build a typed assignment `"col" = $1` with `value` bound to the encoder. */
  def apply[T](col: TypedColumn[T, ?, ?], value: T)(using pf: PgTypeFor[T]): TypedSetAssignment[T, T] = {
    val enc = pf.codec
    val frag: Fragment[T] = Fragment(
      List(Left(s""""${col.name}" = """), Right(enc.sql)),
      enc,
      Origin.unknown
    )
    new TypedSetAssignment[T, T](col, frag, value)
  }

  /** Combine two typed assignments — comma-joined SQL, paired-args `(ArgsL, ArgsR)`. */
  def combine[T1, A1, T2, A2](
    lhs: TypedSetAssignment[T1, A1],
    rhs: TypedSetAssignment[T2, A2]
  ): TypedSetAssignment[Unit, (A1, A2)] = {
    val combinedParts =
      lhs.fragment.parts ++ RawConstants.COMMA_SEP.fragment.parts ++ rhs.fragment.parts
    val combinedEnc = lhs.fragment.encoder.product(rhs.fragment.encoder)
    val frag: Fragment[(A1, A2)] =
      Fragment(combinedParts, combinedEnc, Origin.unknown)
    // Column type isn't meaningful for the combined result — we use `Unit` as a neutral marker; the column
    // ref only matters for single-column assignments (not used downstream on the combined form).
    new TypedSetAssignment[Unit, (A1, A2)](null.asInstanceOf[TypedColumn[Unit, ?, ?]], frag, (lhs.args, rhs.args))
  }

  /** Infix `&` combinator — reads like the commas in a SET list. */
  extension [T1, A1](lhs: TypedSetAssignment[T1, A1])
    def &[T2, A2](rhs: TypedSetAssignment[T2, A2]): TypedSetAssignment[Unit, (A1, A2)] =
      combine(lhs, rhs)

}

extension [T, Null <: Boolean, N <: String & Singleton](col: TypedColumn[T, Null, N])
  /**
   * Typed `:=` overload returning `TypedSetAssignment[T, T]` so `Args = T` is visible to the typed UPDATE
   * pipeline. The existing `:=` returns `SetAssignment[T]` for the untyped path.
   */
  def `:=%`(value: T)(using pf: PgTypeFor[T]): TypedSetAssignment[T, T] =
    TypedSetAssignment(col, value)

/**
 * Typed UPDATE — `UPDATE "t" SET <typed> [WHERE <typed>]` with `Args` visible.
 *
 * The class carries the SET fragment and value (already typed via [[TypedSetAssignment]]) plus an optional
 * WHERE fragment (typed). At `.compile` the SET and WHERE encoders are combined via product so the resulting
 * `Fragment[Args]` carries the full captured tuple in SQL position order: SET-args first, WHERE-args second.
 */
final class TypedUpdateReady[Cols <: Tuple, Name <: String & Singleton, Args] private[dsl] (
  private[dsl] val table:         Table[Cols, Name],
  private[dsl] val setFragment:   Fragment[?],
  private[dsl] val setArgs:       Any,
  private[dsl] val whereFragment: Option[Fragment[?]] = None,
  private[dsl] val whereArgs:     Option[Any]         = None
) {

  /** Add a typed WHERE — extends `Args` to `(Args, WA)`. Repeatable; AND-combined within one WHERE clause. */
  def where[WA](f: skunk.sharp.ColumnsView[Cols] => TypedWhere[WA]): TypedUpdateReady[Cols, Name, (Args, WA)] = {
    val pred = f(table.columnsView)
    whereFragment match {
      case None =>
        new TypedUpdateReady[Cols, Name, (Args, WA)](
          table,
          setFragment,
          setArgs,
          Some(pred.fragment),
          Some(pred.args)
        )
      case Some(wf) =>
        // AND-combine within the WHERE clause: result holds combined fragment whose args are the existing
        // WHERE-args paired with the new typed predicate's args. The builder's outer Args grows accordingly.
        val combinedParts =
          RawConstants.OPEN_PAREN.fragment.parts ++
            wf.parts ++
            RawConstants.AND.fragment.parts ++
            pred.fragment.parts ++
            RawConstants.CLOSE_PAREN.fragment.parts
        val combinedEncoder = wf.encoder.asInstanceOf[skunk.Encoder[Any]].product(pred.fragment.encoder)
        val combinedFragment: Fragment[?] =
          Fragment(combinedParts, combinedEncoder, Origin.unknown).asInstanceOf[Fragment[?]]
        val combinedArgs = (whereArgs.get, pred.args)
        new TypedUpdateReady[Cols, Name, (Args, WA)](
          table,
          setFragment,
          setArgs,
          Some(combinedFragment),
          Some(combinedArgs)
        )
    }
  }

  def compile: CompiledCommand[Args] = {
    val header = table.updateSetHeader

    val (combinedParts, combinedEncoder, combinedArgs) = whereFragment match {
      case None =>
        (
          header.fragment.parts ++ setFragment.parts,
          setFragment.encoder.asInstanceOf[skunk.Encoder[Args]],
          setArgs.asInstanceOf[Args]
        )
      case Some(wf) =>
        val parts =
          header.fragment.parts ++
            setFragment.parts ++
            RawConstants.WHERE.fragment.parts ++
            wf.parts
        val enc = setFragment.encoder.asInstanceOf[skunk.Encoder[Any]].product(wf.encoder)
        val a   = (setArgs, whereArgs.get)
        (parts, enc.asInstanceOf[skunk.Encoder[Args]], a.asInstanceOf[Args])
    }

    val frag: Fragment[Args] = Fragment(combinedParts, combinedEncoder, Origin.unknown)
    CompiledCommand.mk[Args](frag, combinedArgs)
  }

}

extension [Cols <: Tuple, Name <: String & Singleton](b: UpdateBuilder[Cols, Name])
  /**
   * Typed `.set` overload — `f` yields a `TypedSetAssignment[T, A]` (built via the `:=%` typed extension on
   * TypedColumn). Returns a [[TypedUpdateReady]] carrying `Args = A` so `.compile` surfaces
   * `CompiledCommand[A]`.
   *
   * Single-assignment for now; multi-assignment via tuples needs a typed `Tuple => TypedSetAssignment[?, ?]`
   * combinator, deferred.
   */
  def setT[T, A](f: skunk.sharp.ColumnsView[Cols] => TypedSetAssignment[T, A]): TypedUpdateReady[Cols, Name, A] = {
    val a = f(b.table.columnsView)
    new TypedUpdateReady[Cols, Name, A](b.table, a.fragment, a.args)
  }
