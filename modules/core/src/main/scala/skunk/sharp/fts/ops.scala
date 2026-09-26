package skunk.sharp.fts

import skunk.sharp.{PgOperator, TypedExpr}
import skunk.sharp.ops.Stripped
import skunk.sharp.where.Where

/**
 * Full-text search operators, named rather than symbolic (like the rest of the DSL).
 *
 *   - `doc.matches(query)` — `tsvector @@ tsquery`, the search predicate (GIN-indexable).
 *   - `a.concat(b)` — `tsvector || tsvector`.
 *   - `q1.andQuery(q2)` / `q1.orQuery(q2)` / `q.negate` / `q1.followedBy(q2)` — `&&`, `||`, `!!`, `<->` on `tsquery`
 *     (not `and` / `or`, which are the boolean `Where` combinators).
 *
 * `matches` / `concat` also accept a nullable (`Option[TsVector]`) column — e.g. a generated `tsvector` column.
 *
 * The combinators render **parenthesised** (`(q1 && q2)`): `@@`, `&&`, `||`, `!!` and `<->` all share Postgres's "other
 * operator" precedence and associate left, so an unparenthesised `doc @@ q1 && q2` would parse as `(doc @@ q1) && q2`.
 */
extension [T, A](doc: TypedExpr[T, A])(using @annotation.unused ev: Stripped[T] <:< TsVector) {

  /** `doc @@ query` — does the document match the query? */
  inline def matches[B](query: TypedExpr[TsQuery, B]): Where[Where.Concat[A, B]] =
    Where(PgOperator.infix[T, TsQuery, Boolean, A, B]("@@")(doc, query).fragment)

  /** `doc || other` — concatenate two documents (positions of `other` are shifted). */
  inline def concat[U, B](other: TypedExpr[U, B])(using
    Stripped[U] <:< TsVector
  ): TypedExpr[TsVector, Where.Concat[A, B]] =
    paren(PgOperator.infix[T, U, TsVector, A, B]("||")(doc, other))

}

extension [A](q: TypedExpr[TsQuery, A]) {

  /** `q && other` — both queries must match. */
  inline def andQuery[B](other: TypedExpr[TsQuery, B]): TypedExpr[TsQuery, Where.Concat[A, B]] =
    paren(PgOperator.infix[TsQuery, TsQuery, TsQuery, A, B]("&&")(q, other))

  /** `q || other` — either query matches. */
  inline def orQuery[B](other: TypedExpr[TsQuery, B]): TypedExpr[TsQuery, Where.Concat[A, B]] =
    paren(PgOperator.infix[TsQuery, TsQuery, TsQuery, A, B]("||")(q, other))

  /** `!! q` — the query must not match. */
  def negate: TypedExpr[TsQuery, A] = paren(PgOperator.prefix[TsQuery, TsQuery, A]("!! ")(q))

  /** `q <-> other` — `other` must directly follow `q` (phrase search). */
  inline def followedBy[B](other: TypedExpr[TsQuery, B]): TypedExpr[TsQuery, Where.Concat[A, B]] =
    paren(PgOperator.infix[TsQuery, TsQuery, TsQuery, A, B]("<->")(q, other))

}

/** Wrap an operator expression in parentheses (see the precedence note above). */
private def paren[R, A](e: TypedExpr[R, A]): TypedExpr[R, A] =
  TypedExpr[R, A](TypedExpr.wrap("(", e.fragment, ")"), e.codec)
