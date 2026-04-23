package skunk.sharp.dsl

import cats.data.Kleisli
import cats.effect.Resource
import fs2.Stream
import skunk.*
import skunk.data.Completion

/**
 * The compiled form of a query builder — everything needed to execute but nothing session-bound yet. Every DSL verb
 * (`select`, `insert`, `update`, `delete`, plus their `RETURNING` variants) reduces to one of these two types at
 * `.compile` time. All session-facing operations (`run`, `unique`, `option`, `stream`, `cursor`) live as extensions on
 * these types — defined once, available everywhere, mirroring [[skunk.Session]]'s own row-fetching surface.
 *
 * Splitting builders from execution keeps two concerns separate: builders know about the query's structure and
 * refinements, compiled values know about skunk's runtime plumbing. It also lets `RETURNING` variants on
 * INSERT/UPDATE/DELETE share every query operation with SELECT for free.
 *
 * Note on `prepare` / `prepareR`: skunk exposes those so callers can re-bind different argument values on each call.
 * Our `AppliedFragment` bakes arguments in, which defeats the point — so we don't ship `.prepare` extensions here.
 * Users that genuinely need re-bindable prepared queries should build a `skunk.Query[A, B]` directly.
 */
final case class CompiledQuery[R](af: AppliedFragment, codec: Codec[R])

/** The compiled form of a statement that does not return rows (INSERT/UPDATE/DELETE without RETURNING). */
final case class CompiledCommand(af: AppliedFragment)

/**
 * Session-facing operations for queries. Deliberately mirror [[skunk.Session]]'s own row-fetching API so there's one
 * vocabulary to learn.
 */
extension [R](q: CompiledQuery[R]) {

  /** Run and collect all rows. */
  inline def run[F[_]](session: Session[F]): F[List[R]] =
    session.execute(q.af.fragment.query(q.codec))(q.af.argument)

  /** Run and return exactly one row. Fails if the row count is not 1. */
  inline def unique[F[_]](session: Session[F]): F[R] =
    session.unique(q.af.fragment.query(q.codec))(q.af.argument)

  /** Run and return at most one row. Fails if the row count is greater than 1. */
  inline def option[F[_]](session: Session[F]): F[Option[R]] =
    session.option(q.af.fragment.query(q.codec))(q.af.argument)

  /** Stream rows with back-pressure. `chunkSize` is the number of rows fetched per network round-trip. */
  inline def stream[F[_]](session: Session[F], chunkSize: Int = 64): Stream[F, R] =
    session.stream(q.af.fragment.query(q.codec))(q.af.argument, chunkSize)

  /** Open a cursor for manual row-by-row fetching. Resource-safe. */
  inline def cursor[F[_]](session: Session[F]): Resource[F, Cursor[F, R]] =
    session.cursor(q.af.fragment.query(q.codec))(q.af.argument)

  /** Kleisli variant of [[run]] — session injected at the call edge. */
  def runK[F[_]]: Kleisli[F, Session[F], List[R]] = Kleisli(s => run(s))

  /** Kleisli variant of [[unique]]. */
  def uniqueK[F[_]]: Kleisli[F, Session[F], R] = Kleisli(s => unique(s))

  /** Kleisli variant of [[option]]. */
  def optionK[F[_]]: Kleisli[F, Session[F], Option[R]] = Kleisli(s => option(s))

  /**
   * A `Kleisli` whose container is `Stream[F, *]` — i.e., `Session[F] => Stream[F, R]`. Calling `.run(session)` gives a
   * plain `Stream[F, R]`, so multiple streams share a session without any natural-transformation boilerplate:
   * {{{
   *   Stream.resource(pool).flatMap(rooms.findAll.run)    // Stream[IO, RoomRow]
   * }}}
   */
  def streamKF[F[_]](chunkSize: Int = 64): Kleisli[Stream[F, *], Session[F], R] =
    Kleisli(s => stream[F](s, chunkSize))

}

/** Session-facing operations for commands (INSERT/UPDATE/DELETE without RETURNING). */
extension (c: CompiledCommand) {

  /** Execute and return the completion message. */
  inline def run[F[_]](session: Session[F]): F[Completion] =
    session.execute(c.af.fragment.command)(c.af.argument)

  /** Kleisli variant of [[run]] — session injected at the call edge. */
  def runK[F[_]]: Kleisli[F, Session[F], Completion] = Kleisli(s => run(s))

}

/**
 * Subquery support — typeclass-based. A subquery is anything that can produce a [[Codec]]`[T]` plus a deferred SQL
 * thunk:
 *
 *   - a fully `.compile`d query (identity instance),
 *   - a [[ProjectedSelect]]`[Ss, T]` with an explicit projection (compiled on demand),
 *   - a [[SelectBuilder]]`[Ss]` anchored at a single source whose whole-row is `T` (compiled on demand),
 *   - a [[SetOpQuery]]`[T]` built from chained `UNION` / `INTERSECT` / `EXCEPT`.
 *
 * This way users don't have to call `.compile` on inner queries — they pass the builder directly to `.asExpr`,
 * `col.in(...)`, or `Pg.exists(...)` and the outer compilation handles the inner too.
 *
 * Codec extraction (`codec`) is expected to be cheap — no SQL rendered. Rendering (`render`) is a thunk so callers that
 * don't need the fragment yet (like [[SetOpQuery]] chaining) can defer all SQL materialisation to the single outer
 * `.compile` call.
 *
 * Correlated subqueries: build the inner query inside an outer `.select` / `.where` lambda; outer
 * [[skunk.sharp.TypedColumn]]s are in lexical scope, their `render` emits alias-qualified SQL (`"u"."id"`) that
 * correlates at SQL-evaluation time.
 */
sealed trait AsSubquery[Q, T] {
  def codec(q: Q): Codec[T]
  def render(q: Q): () => AppliedFragment
  final def toCompiled(q: Q): CompiledQuery[T] = CompiledQuery(render(q)(), codec(q))
}

object AsSubquery {

  given identity[T]: AsSubquery[CompiledQuery[T], T] = new AsSubquery[CompiledQuery[T], T] {
    def codec(q: CompiledQuery[T]): Codec[T]               = q.codec
    def render(q: CompiledQuery[T]): () => AppliedFragment = () => q.af
  }

  given fromProjected[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, T](using
    ev: skunk.sharp.GroupCoverage[Proj, Groups]
  ): AsSubquery[ProjectedSelect[Ss, Proj, Groups, T], T] =
    new AsSubquery[ProjectedSelect[Ss, Proj, Groups, T], T] {
      def codec(q: ProjectedSelect[Ss, Proj, Groups, T]): Codec[T]               = q.codec
      def render(q: ProjectedSelect[Ss, Proj, Groups, T]): () => AppliedFragment = () => q.compile(using ev).af
    }

  /**
   * Whole-row SelectBuilder → subquery of NamedRow. Relies on the same `IsSingleSource` evidence `.compile` uses.
   *
   * The result-type `R` is split from the `NamedRowOf[C]` computation via a `=:=` constraint so the compiler can
   * satisfy it even when `R` has already been reduced at the call site (e.g. when chaining `.union` → `.intersect` on a
   * `SetOpQuery[R]` whose `R` the earlier step already normalised to the concrete named-tuple form). Matching the two
   * shapes directly — `AsSubquery[SelectBuilder[Ss], NamedRowOf[C]]` — loses that round-trip: Scala won't always expand
   * the match type inside `NamedRowOf` during implicit search.
   */
  given fromSelectBuilder[Ss <: Tuple, C <: Tuple, R](using
    ev: IsSingleSource.Aux[Ss, C],
    eq: R =:= skunk.sharp.NamedRowOf[C]
  ): AsSubquery[SelectBuilder[Ss], R] =
    new AsSubquery[SelectBuilder[Ss], R] {
      def codec(b: SelectBuilder[Ss]): Codec[R] = {
        val entries = b.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]]
        skunk.sharp.internal.rowCodec(entries.head.effectiveCols).asInstanceOf[Codec[R]]
      }
      def render(b: SelectBuilder[Ss]): () => AppliedFragment = () => b.compile(using ev).af
    }

  given fromSetOp[T]: AsSubquery[SetOpQuery[T], T] = new AsSubquery[SetOpQuery[T], T] {
    def codec(q: SetOpQuery[T]): Codec[T]               = q.codec
    def render(q: SetOpQuery[T]): () => AppliedFragment = q.renderFn
  }

  /**
   * A literal [[Values]] fragment is also a subquery: embed it wherever a sub-select would go (INSERT…FROM, set-op
   * operand, scalar `.asExpr`). Declared here because `AsSubquery` is sealed — subclasses / instances that widen
   * through `new` must live with the declaration.
   */
  given fromValues[Cols <: Tuple, Row <: scala.NamedTuple.AnyNamedTuple]: AsSubquery[Values[Cols, Row], Row] =
    new AsSubquery[Values[Cols, Row], Row] {
      def codec(v: Values[Cols, Row]): Codec[Row]             = v.codec
      def render(v: Values[Cols, Row]): () => AppliedFragment = () => v.render
    }

}

/**
 * Lift a subquery-like thing into the [[skunk.sharp.TypedExpr]] vocabulary so it slots in wherever an expression is
 * expected — SELECT projection, WHERE RHS, UPDATE SET, `col.in(q)`, `Pg.exists(q)`, etc.
 */
extension [Q](q: Q) {

  def asExpr[T](using ev: AsSubquery[Q, T]): skunk.sharp.TypedExpr[T] = {
    val thunk = ev.render(q)
    // Rendering is deferred — `thunk()` sits inside `TypedExpr`'s by-name argument and only fires when the
    // outermost `.compile` walks this expression's `.render`. Inner subquery compilation (for builder-shaped
    // `Q`s) happens at that moment, keeping the "single terminal compile" rule intact.
    skunk.sharp.TypedExpr(
      skunk.sharp.TypedExpr.raw("(") |+| thunk() |+| skunk.sharp.TypedExpr.raw(")"),
      ev.codec(q)
    )
  }

}
