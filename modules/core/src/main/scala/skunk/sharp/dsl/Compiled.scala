package skunk.sharp.dsl

import cats.data.Kleisli
import cats.effect.Resource
import fs2.Stream
import skunk.*
import skunk.data.Completion
import skunk.sharp.where.Where

/**
 * The compiled form of a query builder — a typed `Fragment[Args]` plus a row codec, with no values bound. Every
 * DSL verb (`select`, `insert`, `update`, `delete`, plus their `RETURNING` variants) reduces to one of these
 * two types at `.compile` time. All session-facing operations (`run`, `unique`, `option`, `stream`, `cursor`,
 * `prepared`) live as extensions on these types — defined once, available everywhere, mirroring
 * [[skunk.Session]]'s own row-fetching surface.
 *
 * Internal representation is a typed `skunk.Fragment[Args]`. Argument values are supplied at execute time —
 * `q.run(session)(args)` for non-`Void` `Args`, plain `q.run(session)` for `Args = skunk.Void`. The same
 * `QueryTemplate` value can be reused with many different argument tuples; that's the whole point of building
 * the SQL once and re-binding values.
 *
 * For subquery composition (embedding into an outer query) use `AsSubquery` — it sees the typed inner
 * `Fragment[Args]` and threads it into the outer's args slot.
 */
final class QueryTemplate[Args, R] private (
  val fragment: Fragment[Args],
  val codec:    Codec[R]
) {

  /** Typed skunk `Query[Args, R]` — suitable for `session.prepare(q.typedQuery)` to reuse with arg values. */
  def typedQuery: Query[Args, R] = fragment.query(codec)

  /** Identity — `.compile` on an already-compiled query is a no-op. */
  def compile: QueryTemplate[Args, R] = this

  /**
   * Map the row shape into a case class `T` whose `MirroredElemTypes` align with `R`. Works for plain-tuple
   * projections (e.g. from `.returningTuple`) and named-tuple projections (e.g. from `.returningAll`) — the
   * [[QueryTemplateMapping.Unwrap]] match type strips named-tuple labels before the comparison.
   */
  def to[T <: Product](using
    m: scala.deriving.Mirror.ProductOf[T] {
      type MirroredElemTypes = QueryTemplateMapping.Unwrap[R] & Tuple
    }
  ): QueryTemplate[Args, T] = {
    val mapped: skunk.Codec[T] = codec.imap[T](r => m.fromProduct(r.asInstanceOf[Product]))(t =>
      Tuple.fromProductTyped[T](t)(using m).asInstanceOf[R]
    )
    QueryTemplate.mk[Args, T](fragment, mapped)
  }
}

object QueryTemplate {

  /** Direct construction from a typed `Fragment[Args]` + row codec. */
  def mk[Args, R](fragment: Fragment[Args], codec: Codec[R]): QueryTemplate[Args, R] =
    new QueryTemplate[Args, R](fragment, codec)

  /**
   * Bridge for call sites that have an `AppliedFragment` (its args are already baked) and need to surface as a
   * `QueryTemplate[Void, R]`. Used by the few remaining AF-based code paths (e.g. `SetOpQuery.compile`); typed
   * builders should call [[mk]] directly with their `Fragment[Args]`.
   */
  def fromApplied[R](af: AppliedFragment, codec: Codec[R]): QueryTemplate[Void, R] =
    new QueryTemplate[Void, R](skunk.sharp.TypedExpr.liftAfToVoid(af), codec)
}

/** The compiled form of a statement that does not return rows (INSERT/UPDATE/DELETE without RETURNING). */
final class CommandTemplate[Args] private (
  val fragment: Fragment[Args]
) {

  /** Typed skunk `Command[Args]` — suitable for `session.prepare(c.typedCommand)`. */
  def typedCommand: Command[Args] = fragment.command

  /** Identity — `.compile` on an already-compiled command is a no-op. */
  def compile: CommandTemplate[Args] = this
}

object CommandTemplate {

  def mk[Args](fragment: Fragment[Args]): CommandTemplate[Args] =
    new CommandTemplate[Args](fragment)

  def fromApplied(af: AppliedFragment): CommandTemplate[Void] =
    new CommandTemplate[Void](skunk.sharp.TypedExpr.liftAfToVoid(af))
}

/** Strip named-tuple labels for the `to[T]` match-type unification on [[QueryTemplate]]. */
object QueryTemplateMapping {

  type Unwrap[R] = R match
    case scala.NamedTuple.NamedTuple[?, v] => v
    case _                                 => R
}

/**
 * Session-facing operations for queries. Mirror [[skunk.Session]]'s own row-fetching API. `args` is the typed
 * captured-parameter tuple — supplied at execute time, not at builder-build time. For `Args = Void`-shaped
 * templates see the extension block below for argless overloads.
 */
extension [Args, R](q: QueryTemplate[Args, R]) {

  /** Run and collect all rows. */
  inline def run[F[_]](session: Session[F])(args: Args): F[List[R]] =
    session.execute(q.typedQuery)(args)

  /** Run and return exactly one row. Fails if the row count is not 1. */
  inline def unique[F[_]](session: Session[F])(args: Args): F[R] =
    session.unique(q.typedQuery)(args)

  /** Run and return at most one row. Fails if the row count is greater than 1. */
  inline def option[F[_]](session: Session[F])(args: Args): F[Option[R]] =
    session.option(q.typedQuery)(args)

  /** Stream rows with back-pressure. `chunkSize` is the number of rows fetched per network round-trip. */
  inline def stream[F[_]](session: Session[F], chunkSize: Int)(args: Args): Stream[F, R] =
    session.stream(q.typedQuery)(args, chunkSize)

  /** Open a cursor for manual row-by-row fetching. Resource-safe. */
  inline def cursor[F[_]](session: Session[F])(args: Args): Resource[F, Cursor[F, R]] =
    session.cursor(q.typedQuery)(args)

  /** Prepare the query as a [[skunk.PreparedQuery]] for re-execution with different argument values. */
  def prepared[F[_]](session: Session[F]): F[PreparedQuery[F, Args, R]] =
    session.prepare(q.typedQuery)

  /** Bind a specific args value, producing the opaque [[skunk.AppliedFragment]] form. */
  def bind(args: Args): AppliedFragment = q.fragment(args)

  /** Kleisli variant of [[run]] — session injected at the call edge; args bound up front. */
  def runK[F[_]](args: Args): Kleisli[F, Session[F], List[R]] = Kleisli(s => run(s)(args))

  /** Kleisli variant of [[unique]]. */
  def uniqueK[F[_]](args: Args): Kleisli[F, Session[F], R] = Kleisli(s => unique(s)(args))

  /** Kleisli variant of [[option]]. */
  def optionK[F[_]](args: Args): Kleisli[F, Session[F], Option[R]] = Kleisli(s => option(s)(args))

  /**
   * A `Kleisli` whose container is `Stream[F, *]` — i.e., `Session[F] => Stream[F, R]`. Calling `.run(session)` gives a
   * plain `Stream[F, R]`, so multiple streams share a session without any natural-transformation boilerplate.
   */
  def streamKF[F[_]](args: Args, chunkSize: Int): Kleisli[Stream[F, *], Session[F], R] =
    Kleisli(s => stream[F](s, chunkSize)(args))

}

/**
 * Argless overloads for `Args = Void`-shaped templates — no `args` parameter at execute time. Routes through
 * Skunk's extended-protocol execute (`session.execute(q)(Void)`) **not** the simple-protocol `execute(q)` —
 * Void here means "encoder takes Void at execute" which may still emit baked values via `contramap` (e.g.
 * INSERT with values baked via `Param.bind`). The simple protocol can't bind any params; the extended one
 * does. The few truly-no-params cases pay one extra round trip but are still correct.
 */
extension [R](q: QueryTemplate[Void, R]) {

  inline def run[F[_]](session: Session[F]): F[List[R]] =
    session.execute[Void, R](q.typedQuery)(Void)

  inline def unique[F[_]](session: Session[F]): F[R] =
    session.unique[Void, R](q.typedQuery)(Void)

  inline def option[F[_]](session: Session[F]): F[Option[R]] =
    session.option[Void, R](q.typedQuery)(Void)

  inline def stream[F[_]](session: Session[F], chunkSize: Int = 64): Stream[F, R] =
    session.stream(q.typedQuery)(Void, chunkSize)

  inline def cursor[F[_]](session: Session[F]): Resource[F, Cursor[F, R]] =
    session.cursor(q.typedQuery)(Void)

  /** Pre-applied form — same as `.bind(Void)` / `.fragment(Void)`. */
  def af: AppliedFragment = q.fragment(Void)

  def runK[F[_]]: Kleisli[F, Session[F], List[R]] = Kleisli(s => run(s))

  def uniqueK[F[_]]: Kleisli[F, Session[F], R] = Kleisli(s => unique(s))

  def optionK[F[_]]: Kleisli[F, Session[F], Option[R]] = Kleisli(s => option(s))

  def streamKF[F[_]](chunkSize: Int = 64): Kleisli[Stream[F, *], Session[F], R] =
    Kleisli(s => stream[F](s, chunkSize))

}

/** Session-facing operations for commands (INSERT/UPDATE/DELETE without RETURNING). */
extension [Args](c: CommandTemplate[Args]) {

  /** Execute and return the completion message. */
  inline def run[F[_]](session: Session[F])(args: Args): F[Completion] =
    session.execute(c.typedCommand)(args)

  /** Prepare the command as a [[skunk.PreparedCommand]] for re-execution with different argument values. */
  def prepared[F[_]](session: Session[F]): F[PreparedCommand[F, Args]] =
    session.prepare(c.typedCommand)

  def bind(args: Args): AppliedFragment = c.fragment(args)

  def runK[F[_]](args: Args): Kleisli[F, Session[F], Completion] = Kleisli(s => run(s)(args))

}

/** Argless command overloads for `Args = Void`. Routes through extended protocol (see QueryTemplate Void). */
extension (c: CommandTemplate[Void]) {

  inline def run[F[_]](session: Session[F]): F[Completion] =
    session.execute[Void](c.typedCommand)(Void)

  def af: AppliedFragment = c.fragment(Void)

  def runK[F[_]]: Kleisli[F, Session[F], Completion] = Kleisli(s => run(s))

}

/**
 * Subquery support — typeclass-based. A subquery is anything that can produce a [[Codec]]`[T]` plus a typed
 * `Fragment[Args]`:
 *
 *   - a fully `.compile`d query (identity instance),
 *   - a [[ProjectedSelect]] with an explicit projection (compiled on demand),
 *   - a [[SelectBuilder]] anchored at a single source whose whole-row is `T` (compiled on demand),
 *   - a [[SetOpQuery]] built from chained `UNION` / `INTERSECT` / `EXCEPT`,
 *   - a literal [[Values]] table.
 *
 * `Args` exposes the inner subquery's typed parameters so they thread into the outer composition wherever the
 * subquery is embedded — in `Pg.exists(inner)`, `col.in(inner)`, scalar `.asExpr`, set-op operands, etc.
 *
 * Codec extraction (`codec`) is expected to be cheap — no SQL rendered. `fragment` is allowed to compile the
 * inner query (cheap for builders, no extra cost over `.compile`).
 */
sealed trait AsSubquery[Q, T, Args] {
  def codec(q: Q): Codec[T]
  def fragment(q: Q): Fragment[Args]
}

object AsSubquery {

  given identity[Args, T]: AsSubquery[QueryTemplate[Args, T], T, Args] =
    new AsSubquery[QueryTemplate[Args, T], T, Args] {
      def codec(q: QueryTemplate[Args, T]): Codec[T]       = q.codec
      def fragment(q: QueryTemplate[Args, T]): Fragment[Args] = q.fragment
    }

  given fromProjected[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, PA, WA, HA, T](using
    ev:   skunk.sharp.GroupCoverage[Proj, Groups],
    pa:   skunk.sharp.dsl.ProjArgsOf.Aux[Proj, PA],
    c12:  Where.Concat2[PA, WA],
    c123: Where.Concat2[Where.Concat[PA, WA], HA]
  ): AsSubquery[ProjectedSelect[Ss, Proj, Groups, WA, HA, T], T, Where.Concat[Where.Concat[PA, WA], HA]] =
    new AsSubquery[ProjectedSelect[Ss, Proj, Groups, WA, HA, T], T, Where.Concat[Where.Concat[PA, WA], HA]] {
      def codec(q: ProjectedSelect[Ss, Proj, Groups, WA, HA, T]): Codec[T] = q.codec
      def fragment(q: ProjectedSelect[Ss, Proj, Groups, WA, HA, T]): Fragment[Where.Concat[Where.Concat[PA, WA], HA]] =
        q.compile[PA](using ev, pa, c12, c123).fragment
    }

  /**
   * Whole-row SelectBuilder → subquery of NamedRow. Relies on the same `IsSingleSource` evidence `.compile` uses.
   */
  given fromSelectBuilder[Ss <: Tuple, WA, HA, C <: Tuple, R](using
    ev: IsSingleSource.Aux[Ss, C],
    c2: Where.Concat2[WA, HA],
    eq: R =:= skunk.sharp.NamedRowOf[C]
  ): AsSubquery[SelectBuilder[Ss, WA, HA], R, Where.Concat[WA, HA]] =
    new AsSubquery[SelectBuilder[Ss, WA, HA], R, Where.Concat[WA, HA]] {
      def codec(b: SelectBuilder[Ss, WA, HA]): Codec[R] = {
        val entries = b.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
        skunk.sharp.internal.rowCodec(entries.head.effectiveCols).asInstanceOf[Codec[R]]
      }
      def fragment(b: SelectBuilder[Ss, WA, HA]): Fragment[Where.Concat[WA, HA]] =
        b.compile(using ev, c2).fragment.asInstanceOf[Fragment[Where.Concat[WA, HA]]]
    }

  given fromSetOp[T]: AsSubquery[SetOpQuery[T], T, Void] =
    new AsSubquery[SetOpQuery[T], T, Void] {
      def codec(q: SetOpQuery[T]): Codec[T]               = q.codec
      def fragment(q: SetOpQuery[T]): Fragment[Void] =
        skunk.sharp.TypedExpr.liftAfToVoid(q.renderFn())
    }

  /**
   * A literal [[Values]] fragment is also a subquery: embed it wherever a sub-select would go (INSERT…FROM, set-op
   * operand, scalar `.asExpr`).
   */
  given fromValues[Cols <: Tuple, Row <: scala.NamedTuple.AnyNamedTuple]: AsSubquery[Values[Cols, Row], Row, Void] =
    new AsSubquery[Values[Cols, Row], Row, Void] {
      def codec(v: Values[Cols, Row]): Codec[Row] = v.codec
      def fragment(v: Values[Cols, Row]): Fragment[Void] =
        skunk.sharp.TypedExpr.liftAfToVoid(v.render)
    }

}

/**
 * Lift a subquery-like thing into the [[skunk.sharp.TypedExpr]] vocabulary so it slots in wherever an expression is
 * expected — SELECT projection, WHERE RHS, UPDATE SET, `col.in(q)`, `Pg.exists(q)`, etc. The inner subquery's `Args`
 * threads into the resulting `TypedExpr[T, Args]`.
 */
extension [Q](q: Q) {

  def asExpr[T, Args](using ev: AsSubquery[Q, T, Args]): skunk.sharp.TypedExpr[T, Args] = {
    val inner = ev.fragment(q)
    val frag  = skunk.sharp.TypedExpr.wrap("(", inner, ")")
    skunk.sharp.TypedExpr(frag, ev.codec(q))
  }

}
