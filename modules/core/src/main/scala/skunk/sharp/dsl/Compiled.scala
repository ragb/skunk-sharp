package skunk.sharp.dsl

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

}

/** Session-facing operations for commands (INSERT/UPDATE/DELETE without RETURNING). */
extension (c: CompiledCommand) {

  /** Execute and return the completion message. */
  inline def run[F[_]](session: Session[F]): F[Completion] =
    session.execute(c.af.fragment.command)(c.af.argument)

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

}

/**
 * Lift a subquery-like thing into the [[skunk.sharp.TypedExpr]] vocabulary so it slots in wherever an expression is
 * expected — SELECT projection, WHERE RHS, UPDATE SET, `col.in(q)`, `Pg.exists(q)`, etc.
 */
extension [Q](q: Q) {

  def asExpr[T](using ev: AsSubquery[Q, T]): skunk.sharp.TypedExpr[T] =
    skunk.sharp.TypedExpr(
      skunk.sharp.TypedExpr.raw("(") |+| ev.render(q)() |+| skunk.sharp.TypedExpr.raw(")"),
      ev.codec(q)
    )

}
