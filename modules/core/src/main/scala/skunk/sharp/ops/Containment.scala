package skunk.sharp.ops

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/*
 * The operators several Postgres types share under one symbol — `@>` / `<@` (containment), `&&` (overlap), `||`
 * (concatenation) — as one set of extension methods, dispatched by typeclass. Each type ships its instances in its own
 * companion (arrays and ranges below; `Jsonb`, `Hstore`, `LTree`, `TsVector` in theirs), so the methods can all be
 * exported from `skunk.sharp.dsl` without the name clashes per-type extensions had.
 *
 * Like the arithmetic typeclasses, there is no shared supertrait: an instance for one operator is never evidence for
 * another.
 */

/** `L @> R` (and `R <@ L`) is meaningful. */
sealed trait Contains[L, R]

object Contains {
  private val instance: Contains[Any, Any] = new Contains[Any, Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[L, R]: Contains[L, R] = instance.asInstanceOf[Contains[L, R]]

  given arrays[A](using skunk.sharp.pg.IsArray[A]): Contains[A, A]              = of
  given ranges[R](using skunk.sharp.pg.IsRange[R]): Contains[R, R]              = of
  given rangeElem[R, E](using skunk.sharp.pg.IsRange.Aux[R, E]): Contains[R, E] = of
}

/** `L && R` is meaningful. */
sealed trait Overlaps[L, R]

object Overlaps {
  private val instance: Overlaps[Any, Any] = new Overlaps[Any, Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[L, R]: Overlaps[L, R] = instance.asInstanceOf[Overlaps[L, R]]

  given arrays[A](using skunk.sharp.pg.IsArray[A]): Overlaps[A, A] = of
  given ranges[R](using skunk.sharp.pg.IsRange[R]): Overlaps[R, R] = of
}

/** `L || R` is meaningful, with result type `Out`. */
sealed trait Concatenable[L, R] { type Out }

object Concatenable {
  type Aux[L, R, O] = Concatenable[L, R] { type Out = O }
  private val instance: Concatenable[Any, Any] = new Concatenable[Any, Any] {}

  /** Build an instance — the extension point for types outside core. */
  def of[L, R, O]: Aux[L, R, O] = instance.asInstanceOf[Aux[L, R, O]]

  given arrays[A](using skunk.sharp.pg.IsArray[A]): Aux[A, A, A] = of
}

extension [L, A](lhs: TypedExpr[L, A]) {

  /** `(lhs @> rhs)` — arrays, ranges (incl. a range containing an element), jsonb, hstore, ltree ancestors, … */
  inline def contains[R, B](rhs: TypedExpr[R, B])(using
    @annotation.unused ev: Contains[Stripped[L], Stripped[R]]
  ): Where[Where.Concat[A, B]] =
    Where(PgOperator.binary[A, B]("@>", lhs.fragment, rhs.fragment))

  /** `(lhs <@ rhs)` — the converse of [[contains]]. */
  inline def containedBy[R, B](rhs: TypedExpr[R, B])(using
    @annotation.unused ev: Contains[Stripped[R], Stripped[L]]
  ): Where[Where.Concat[A, B]] =
    Where(PgOperator.binary[A, B]("<@", lhs.fragment, rhs.fragment))

  /** `(lhs && rhs)` — arrays and ranges share an element / a point. */
  inline def overlaps[R, B](rhs: TypedExpr[R, B])(using
    @annotation.unused ev: Overlaps[Stripped[L], Stripped[R]]
  ): Where[Where.Concat[A, B]] =
    Where(PgOperator.binary[A, B]("&&", lhs.fragment, rhs.fragment))

  /** `(lhs || rhs)` — arrays, jsonb, ltree paths, tsvector documents, … Nullable when either side is. */
  inline def concat[R, B, O](rhs: TypedExpr[R, B])(using
    @annotation.unused ev: Concatenable.Aux[Stripped[L], Stripped[R], O],
    pf: PgTypeFor[Lift2[L, R, O]]
  ): TypedExpr[Lift2[L, R, O], Where.Concat[A, B]] =
    TypedExpr[Lift2[L, R, O], Where.Concat[A, B]](PgOperator.binary[A, B]("||", lhs.fragment, rhs.fragment), pf.codec)

}
