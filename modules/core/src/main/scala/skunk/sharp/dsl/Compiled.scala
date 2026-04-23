package skunk.sharp.dsl

import cats.data.Kleisli
import cats.effect.Resource
import fs2.Stream
import skunk.*
import skunk.data.Completion

/**
 * The compiled form of a query builder — everything needed to execute but nothing session-bound yet. Every DSL verb
 * (`select`, `insert`, `update`, `delete`, plus their `RETURNING` variants) reduces to one of these two types at
 * `.compile` time. All session-facing operations (`run`, `unique`, `option`, `stream`, `cursor`, `prepared`) live as
 * extensions on these types — defined once, available everywhere, mirroring [[skunk.Session]]'s own row-fetching
 * surface.
 *
 * Internal representation is a **typed** `skunk.Fragment[Args]` paired with its captured `args: Args`. `Args` is a
 * visible type parameter: call sites that want it concrete (because the builder chain threaded it, or because the
 * caller constructed via `CompiledQuery.mk`) see the real tuple; call sites that plumbed through `AppliedFragment`
 * on the way in see `af.A` — still typed, just not nameable at the call site. Either form works with
 * `q.typedQuery` / `c.typedCommand` and `session.prepare(…)` to re-bind different argument values later.
 * `.af: AppliedFragment` remains as a derived `lazy val` for subquery composition.
 */
final class CompiledQuery[Args, R] private (
  val fragment: Fragment[Args],
  val args:     Args,
  val codec:    Codec[R]
) {

  /** Pre-applied form: `Fragment[Args].apply(Args)` — the opaque AppliedFragment. Cached. Use for subquery embedding. */
  lazy val af: AppliedFragment = fragment(args)

  /** Typed skunk `Query[Args, R]` — suitable for `session.prepare(q.typedQuery)` to reuse with different arg values. */
  def typedQuery: Query[Args, R] = fragment.query(codec)
}

object CompiledQuery {

  /**
   * Construct a `CompiledQuery[Args, R]` from an already-assembled `AppliedFragment` + row codec. The existing
   * call sites that construct a `CompiledQuery` from an AppliedFragment lose the specific Args type — we recover
   * it via the path-dependent `af0.A` and forward into the `Args` parameter as a wildcard-like existential.
   *
   * Call sites that want a **concrete** visible Args tuple (so the user can ascribe
   * `val q: CompiledQuery[(Int, String), Row]`) should construct via `mk` and pass the typed Fragment + args
   * directly rather than going through AppliedFragment — typically from a macro that knows both pieces.
   */
  def apply[R](af0: AppliedFragment, codec0: Codec[R]): CompiledQuery[af0.A, R] =
    new CompiledQuery[af0.A, R](af0.fragment, af0.argument, codec0)

  /** Direct construction when the caller already has a typed fragment + args (skipping AppliedFragment). */
  def mk[Args, R](fragment: Fragment[Args], args: Args, codec: Codec[R]): CompiledQuery[Args, R] =
    new CompiledQuery[Args, R](fragment, args, codec)
}

/** The compiled form of a statement that does not return rows (INSERT/UPDATE/DELETE without RETURNING). */
final class CompiledCommand[Args] private (
  val fragment: Fragment[Args],
  val args:     Args
) {

  /** Cached pre-applied form. */
  lazy val af: AppliedFragment = fragment(args)

  /** Typed skunk `Command[Args]` — suitable for `session.prepare(c.typedCommand)` to reuse. */
  def typedCommand: Command[Args] = fragment.command
}

object CompiledCommand {

  def apply(af0: AppliedFragment): CompiledCommand[af0.A] =
    new CompiledCommand[af0.A](af0.fragment, af0.argument)

  def mk[Args](fragment: Fragment[Args], args: Args): CompiledCommand[Args] =
    new CompiledCommand[Args](fragment, args)
}

/**
 * Session-facing operations for queries. Deliberately mirror [[skunk.Session]]'s own row-fetching API so there's one
 * vocabulary to learn.
 */
extension [Args, R](q: CompiledQuery[Args, R]) {

  /** Run and collect all rows. */
  inline def run[F[_]](session: Session[F]): F[List[R]] =
    session.execute(q.fragment.query(q.codec))(q.args)

  /** Run and return exactly one row. Fails if the row count is not 1. */
  inline def unique[F[_]](session: Session[F]): F[R] =
    session.unique(q.fragment.query(q.codec))(q.args)

  /** Run and return at most one row. Fails if the row count is greater than 1. */
  inline def option[F[_]](session: Session[F]): F[Option[R]] =
    session.option(q.fragment.query(q.codec))(q.args)

  /** Stream rows with back-pressure. `chunkSize` is the number of rows fetched per network round-trip. */
  inline def stream[F[_]](session: Session[F], chunkSize: Int = 64): Stream[F, R] =
    session.stream(q.fragment.query(q.codec))(q.args, chunkSize)

  /** Open a cursor for manual row-by-row fetching. Resource-safe. */
  inline def cursor[F[_]](session: Session[F]): Resource[F, Cursor[F, R]] =
    session.cursor(q.fragment.query(q.codec))(q.args)

  /**
   * Prepare the query as a skunk [[skunk.PreparedQuery]] for re-execution with **different** argument values.
   *
   * The `Args` parameter comes from the path-dependent `q.Args`: users get a `PreparedQuery[F, Args, R]` whose
   * input tuple matches what the DSL captured. For a query that captured a single `Int` via `u.age >= 18`, that
   * `Args` is `Int *: EmptyTuple` — a tuple type a real call site can ascribe. Today this is the cleanest path
   * out of AppliedFragment's opaque arg bag.
   */
  def prepared[F[_]](session: Session[F]): F[PreparedQuery[F, Args, R]] =
    session.prepare(q.typedQuery)

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
extension [Args](c: CompiledCommand[Args]) {

  /** Execute and return the completion message. */
  inline def run[F[_]](session: Session[F]): F[Completion] =
    session.execute(c.fragment.command)(c.args)

  /** Prepare the command as a skunk [[skunk.PreparedCommand]] for re-execution with different argument values. */
  def prepared[F[_]](session: Session[F]): F[PreparedCommand[F, Args]] =
    session.prepare(c.typedCommand)

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
  final def toCompiled(q: Q): CompiledQuery[?, T] = CompiledQuery(render(q)(), codec(q))
}

object AsSubquery {

  given identity[T]: AsSubquery[CompiledQuery[?, T], T] = new AsSubquery[CompiledQuery[?, T], T] {
    def codec(q: CompiledQuery[?, T]): Codec[T]               = q.codec
    def render(q: CompiledQuery[?, T]): () => AppliedFragment = () => q.af
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
