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
