package skunk.sharp.dsl

import skunk.Void
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Typeclass that computes the runtime `Out` type for a projection-shaped value `T` and provides a
 * runtime projector that turns an `Out` value into a list of per-item values (one per inner
 * `TypedExpr` slot in render order).
 *
 * Lets us compute the typed `Args` for SELECT projections, GROUP BY / ORDER BY / DISTINCT ON, and
 * other multi-item DSL positions WITHOUT relying on match types — given-resolution priority
 * disambiguates `NamedTuple` vs regular `Tuple` vs single `TypedExpr` cases, where match types
 * stumble on Scala 3.8's NamedTuple opaque-type disjointness rules.
 *
 * `T` is contravariant so a `ProjArgsOf[TypedExpr[T, A]]` instance also serves a `TypedColumn[T, N, S]`
 * (which extends `TypedExpr`). Without contravariance, given resolution would need an explicit
 * instance for every `TypedExpr` subtype.
 *
 * Given priority chain:
 *
 *   - High (`ProjArgsOf` companion): `NamedTuple[N, V]`, `EmptyTuple`, `H *: EmptyTuple`.
 *   - Medium: multi-item tuple cons `H *: T` (folds via [[Where.Concat]]).
 *   - Low: any `TypedExpr[T, A]` — leaf case, `Out = A`.
 *
 * `Out` is the right-fold of `Where.Concat` over per-item `Args` — the same shape as
 * [[Where.FoldConcat]] for plain tuples — but computed via given resolution rather than match-type
 * reduction, so named tuples work too.
 */
trait ProjArgsOf[-T] {
  type Out

  /** Project an `Out` value into a flat list of per-item values, in tuple/render order. */
  def project(c: Out): List[Any]
}

object ProjArgsOf extends ProjArgsOfMedPrio {

  type Aux[T, O] = ProjArgsOf[T] { type Out = O }

  /** Strip names from a `NamedTuple` and recurse on the underlying value tuple. */
  given namedTuple[N <: Tuple, V <: Tuple, VOut](using
    inner: ProjArgsOf[V] { type Out = VOut }
  ): (ProjArgsOf[scala.NamedTuple.NamedTuple[N, V]] { type Out = VOut }) =
    new ProjArgsOf[scala.NamedTuple.NamedTuple[N, V]] {
      type Out = VOut
      def project(c: Out): List[Any] = inner.project(c)
    }

  /** No items: `Out = Void`, projects to an empty list. */
  given emptyTuple: (ProjArgsOf[EmptyTuple] { type Out = Void }) =
    new ProjArgsOf[EmptyTuple] {
      type Out = Void
      def project(c: Void): List[Any] = Nil
    }

  /** Single-item tuple: `Out` matches the head's `Out` exactly (no nesting). */
  given singleTuple[H, HOut](using
    h: ProjArgsOf[H] { type Out = HOut }
  ): (ProjArgsOf[H *: EmptyTuple] { type Out = HOut }) =
    new ProjArgsOf[H *: EmptyTuple] {
      type Out = HOut
      def project(c: Out): List[Any] = h.project(c)
    }

}

trait ProjArgsOfMedPrio extends ProjArgsOfLowPrio {

  /** Multi-item tuple cons: right-fold via [[Where.Concat]] (drops Void slots). */
  given consTuple[H, T <: NonEmptyTuple, HOut, TOut](using
    h:  ProjArgsOf[H] { type Out = HOut },
    t:  ProjArgsOf[T] { type Out = TOut },
    c2: Where.Concat2[HOut, TOut]
  ): (ProjArgsOf[H *: T] { type Out = Where.Concat[HOut, TOut] }) =
    new ProjArgsOf[H *: T] {
      type Out = Where.Concat[HOut, TOut]
      def project(c: Out): List[Any] = {
        val (a, b) = c2.project(c)
        h.project(a) ++ t.project(b)
      }
    }

}

trait ProjArgsOfLowPrio {

  /**
   * Leaf case: `TypedExpr[T, A]` (covers `TypedColumn` via contravariance). `Out = A` — the inner
   * Args slot. The runtime projector emits `List(c)` — a single value to feed the underlying encoder.
   */
  given typedExpr[T, A]: (ProjArgsOf[TypedExpr[T, A]] { type Out = A }) =
    new ProjArgsOf[TypedExpr[T, A]] {
      type Out = A
      def project(c: A): List[Any] = List(c)
    }

}
