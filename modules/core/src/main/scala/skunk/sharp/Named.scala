package skunk.sharp

import skunk.sharp.where.Where

import scala.compiletime.{constValue, constValueTuple, erasedValue, summonInline}

/**
 * Args marker for a **named** parameter: `Param.named["buildingId", UUID]` is a `TypedExpr[UUID, Named["buildingId",
 * UUID]]`. Inside the DSL it is an ordinary positional slot (every occurrence gets its own `$n`); only the execute-time
 * surface changes — when every slot of a compiled statement is named, `run` / `unique` / … take a **named tuple** with
 * one field per distinct name, so a value used several times is passed once and same-typed values can't be swapped.
 *
 * A real (tiny) wrapper class: [[NamedArgs.toSlots]] wraps each named slot's value and the parameter's encoder unwraps
 * it, so every cast the DSL inlines against the Args type is sound. Being a final class also lets match types prove it
 * disjoint from `Void`, tuples and plain value types.
 */
final class Named[L <: String & Singleton, T](val value: T)

object NamedArgs {

  /** The slot shape of an Args type: `Void` → `EmptyTuple`, scalar → 1-tuple, tuple → itself. */
  type Slots[A] = Where.AsTuple[A]

  /** `true` iff every slot is a [[Named]] (and there is at least one). */
  type AllNamed[S <: Tuple] <: Boolean = S match {
    case EmptyTuple                => false
    case Named[l, t] *: EmptyTuple => true
    case Named[l, t] *: tail       => AllNamed[tail]
    case _                         => false
  }

  /** `true` iff at least one slot is a [[Named]]. */
  type AnyNamed[S <: Tuple] <: Boolean = S match {
    case EmptyTuple          => false
    case Named[l, t] *: tail => true
    case h *: tail           => AnyNamed[tail]
  }

  /** Per-slot flag: is this slot named? */
  type NamedFlags[S <: Tuple] <: Tuple = S match {
    case EmptyTuple          => EmptyTuple
    case Named[l, t] *: tail => true *: NamedFlags[tail]
    case h *: tail           => false *: NamedFlags[tail]
  }

  /** Slot labels, in slot order (repeats included). */
  type LabelsOf[S <: Tuple] <: Tuple = S match {
    case EmptyTuple          => EmptyTuple
    case Named[l, t] *: tail => l *: LabelsOf[tail]
  }

  /** Slot value types, in slot order. */
  type TypesOf[S <: Tuple] <: Tuple = S match {
    case EmptyTuple          => EmptyTuple
    case Named[l, t] *: tail => t *: TypesOf[tail]
  }

  /** Keep the first slot for each label. */
  type Distinct[S <: Tuple, Seen <: Tuple] <: Tuple = S match {
    case EmptyTuple          => EmptyTuple
    case Named[l, t] *: tail =>
      Contains[l, Seen] match {
        case true  => Distinct[tail, Seen]
        case false => Named[l, t] *: Distinct[tail, l *: Seen]
      }
  }

  /** Value type declared for label `L` (first occurrence). */
  type TypeFor[L, S <: Tuple] = S match {
    case Named[L, t] *: tail => t
    case h *: tail           => TypeFor[L, tail]
  }

  /** Strip [[Named]] markers — the positional form, where a named slot takes a bare `T`. */
  type Erase[A] = A match {
    case Named[l, t] => t
    case Tuple       => EraseAll[A & Tuple]
    case _           => A
  }

  type EraseAll[S <: Tuple] <: Tuple = S match {
    case EmptyTuple => EmptyTuple
    case h *: tail  => Erase[h] *: EraseAll[tail]
  }

  /**
   * What a compiled statement's execute methods take: a named tuple (one field per distinct label) when every slot is
   * named; otherwise the positional Args, with any named slots taking their bare `T`.
   */
  type RunArgs[A] = AllNamed[Slots[A]] match {
    case true =>
      scala.NamedTuple.NamedTuple[LabelsOf[Distinct[Slots[A], EmptyTuple]], TypesOf[Distinct[Slots[A], EmptyTuple]]]
    case false => Erase[A]
  }

  /** Evidence that every occurrence of label `L` has the same value type. */
  @scala.annotation.implicitNotFound(
    "skunk-sharp: named parameter ${L} is used with different types (${T} and ${U}) — give them different names"
  )
  sealed trait SameType[L, T, U]

  object SameType {
    private val instance: SameType[Any, Any, Any] = new SameType[Any, Any, Any] {}
    given same[L, T]: SameType[L, T, T]           = instance.asInstanceOf[SameType[L, T, T]]
  }

  private inline def requireConsistent[S <: Tuple, D <: Tuple]: Unit =
    inline erasedValue[S] match {
      case _: EmptyTuple            => ()
      case _: (Named[l, t] *: tail) =>
        summonInline[SameType[l, t, TypeFor[l, D]]]
        requireConsistent[tail, D]
    }

  /**
   * Convert execute-time arguments to the slot values the statement's encoder expects:
   *
   *   - no named slot: the arguments as they are (no work, no allocation);
   *   - all named: each slot reads the named-tuple field of its label, wrapped in [[Named]];
   *   - mixed: positional, with the named slots' bare values wrapped in [[Named]].
   */
  inline def toSlots[A](args: RunArgs[A]): A =
    inline if !constValue[AnyNamed[Slots[A]]] then args.asInstanceOf[A]
    else inline if constValue[AllNamed[Slots[A]]] then {
      requireConsistent[Slots[A], Distinct[Slots[A], EmptyTuple]]
      val slotLabels = constValueTuple[LabelsOf[Slots[A]]].toList
      val distinct   = constValueTuple[LabelsOf[Distinct[Slots[A], EmptyTuple]]].toList
      val in         = args.asInstanceOf[Product]
      pack[A](slotLabels.map(l => new Named[Nothing, Any](in.productElement(distinct.indexOf(l)))))
    } else {
      val flags  = constValueTuple[NamedFlags[Slots[A]]].toList.asInstanceOf[List[Boolean]]
      val values = if (flags.sizeIs == 1) List[Any](args) else args.asInstanceOf[Product].productIterator.toList
      pack[A](values.zip(flags).map((v, named) => if named then new Named[Nothing, Any](v) else v))
    }

  @scala.annotation.publicInBinary
  private[sharp] def pack[A](values: List[Any]): A =
    (if (values.sizeIs == 1) values.head else Tuple.fromArray(values.toArray[Any])).asInstanceOf[A]

}
