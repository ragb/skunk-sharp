package skunk.sharp.internal

import skunk.sharp.Column
import skunk.sharp.pg.{PgTypeFor, PgTypes}

import scala.util.NotGiven

/**
 * Typeclass dispatch for deriving column tuples from a `Mirror.ProductOf[T]`'s `(Labels, Types)` pair.
 *
 * Why a typeclass instead of a match type? The natural match-type implementation dispatches on `Option[t]` head vs.
 * non-`Option` head, and Scala 3 requires the non-`Option` branch's scrutinee to be **provably disjoint** from
 * `Option`. That works for concrete types (`String`, `Int`), and for opaque subtypes with an explicit upper bound
 * (iron's `opaque type IronType[A, T] <: A = A` — `IronType[String, C]` is provably `<: String`, disjoint from
 * `Option`). It **fails** for unbounded opaque aliases like refined's `opaque type Refined[T, P] = T` (no upper bound
 * visible outside the defining scope — Scala can't prove `Refined[String, C]` is not an `Option`, reduction stalls).
 *
 * Typeclass resolution uses `<:<` directly, which CAN see through opaque types correctly. `NotGiven[T <:< Option[?]]`
 * fires for any `T` not provably an `Option`, which covers unbounded opaques, concrete classes, iron types, and so on.
 * The four instances below form an induction over the label/type tuples.
 */
sealed trait DeriveColumns[Labels <: Tuple, Types <: Tuple] {
  type Out <: Tuple
  def value: Out
}

object DeriveColumns {

  type Aux[Labels <: Tuple, Types <: Tuple, Out0 <: Tuple] = DeriveColumns[Labels, Types] { type Out = Out0 }

  given empty: DeriveColumns.Aux[EmptyTuple, EmptyTuple, EmptyTuple] = new DeriveColumns[EmptyTuple, EmptyTuple] {
    type Out = EmptyTuple
    def value: EmptyTuple = EmptyTuple
  }

  /**
   * Head element is `Option[T]` → the column is nullable, carries `Option[T]` at the Scala level, codec wrapped with
   * `.opt`. Higher priority than [[nonOptionCase]] because its bound on the head is more specific.
   */
  given optionCase[L <: String & Singleton, T, Ls <: Tuple, Ts <: Tuple, Rest <: Tuple](using
    label: ValueOf[L],
    pf: PgTypeFor[T],
    rest: DeriveColumns.Aux[Ls, Ts, Rest]
  ): DeriveColumns.Aux[L *: Ls, Option[T] *: Ts, Column[Option[T], L, true, EmptyTuple] *: Rest] =
    new DeriveColumns[L *: Ls, Option[T] *: Ts] {
      type Out = Column[Option[T], L, true, EmptyTuple] *: Rest

      def value: Out = {
        val col = Column[Option[T], L, true, EmptyTuple](
          name = label.value,
          tpe = PgTypes.typeOf(pf.codec),
          codec = pf.codec.opt,
          isNullable = true,
          hasDefault = false,
          isPrimary = false,
          isUnique = false,
          uniqueGroups = Set.empty
        )
        (col *: rest.value).asInstanceOf[Out]
      }
    }

  /**
   * Head element is *not* `Option[_]` — non-nullable column. `NotGiven[T <:< Option[?]]` guards resolution. Unbounded
   * opaque types pass this guard (no `<:<` exists), concrete non-Option classes pass, iron's subtype-bounded opaque
   * types pass.
   */
  given nonOptionCase[L <: String & Singleton, T, Ls <: Tuple, Ts <: Tuple, Rest <: Tuple](using
    label: ValueOf[L],
    pf: PgTypeFor[T],
    notOption: NotGiven[T <:< Option[?]],
    rest: DeriveColumns.Aux[Ls, Ts, Rest]
  ): DeriveColumns.Aux[L *: Ls, T *: Ts, Column[T, L, false, EmptyTuple] *: Rest] =
    new DeriveColumns[L *: Ls, T *: Ts] {
      type Out = Column[T, L, false, EmptyTuple] *: Rest

      def value: Out = {
        val col = Column[T, L, false, EmptyTuple](
          name = label.value,
          tpe = PgTypes.typeOf(pf.codec),
          codec = pf.codec,
          isNullable = false,
          hasDefault = false,
          isPrimary = false,
          isUnique = false,
          uniqueGroups = Set.empty
        )
        (col *: rest.value).asInstanceOf[Out]
      }
    }

}
