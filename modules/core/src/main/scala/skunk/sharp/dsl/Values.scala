package skunk.sharp.dsl

import cats.Reducible
import skunk.{AppliedFragment, Codec, Fragment}
import skunk.sharp.*
import skunk.sharp.internal.{rowCodec, tupleCodec, DeriveColumns}
import skunk.util.Origin

import scala.NamedTuple

/**
 * A literal `VALUES` table — one or more rows given as named tuples. A single concept unifies two use cases:
 *
 *   - **As a subquery**: supplied to `.insert.from(…)`, `.asExpr`, `col.in(…)`, set-op operands, etc. Satisfies
 *     [[AsSubquery]] so any position accepting a query accepts a `Values[Cols, Row]` with zero ceremony.
 *   - **As a joinable [[Relation]]**: `Values.of(...).alias("v")` is a `Relation[Cols] { type Alias = "v" }` and flows
 *     into `.innerJoin` / `.leftJoin` / `.crossJoin` / `.select` through the same `AsRelation` typeclass as Tables and
 *     Views. The rendered FROM is `(VALUES (…), (…)) AS "v" ("col1", "col2", …)`, including the column list after the
 *     alias — Postgres requires it and the `DeriveColumns`-derived `Cols` gives us the names for free.
 *
 * Codecs come from [[DeriveColumns]] (the same machinery `Table.of[T]` uses) — each named-tuple field is resolved to
 * its [[skunk.sharp.pg.PgTypeFor]] instance. Fields with `Option[T]` values become nullable columns; everything else is
 * non-nullable. Users can override per-field codecs by using the tag subtype aliases in [[skunk.sharp.pg.tags]]
 * (`Varchar[256]`, `Numeric[10, 2]`, …) in the named-tuple field types.
 */
final class Values[Cols <: Tuple, Row <: NamedTuple.AnyNamedTuple] @scala.annotation.publicInBinary private[sharp] (
  private[sharp] val columns: Cols,
  private[sharp] val rowData: List[List[Any]]
) {

  /**
   * Render the parameterised `VALUES (…), (…), …` fragment. Each field of every row flows through its column's codec,
   * so parameters end up bound to the outer query's argument list when this value is embedded (subquery, FROM position,
   * INSERT…FROM source).
   */
  private[sharp] def render: AppliedFragment = {
    val cs          = columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val perRow      = tupleCodec(cs.map(_.codec))
    val rowEnc      = perRow.values
    val rowFrag     = Fragment(List(Right(rowEnc.sql)), rowEnc, Origin.unknown)
    val appliedRows = rowData.map(r => rowFrag(Tuple.fromArray(r.toArray[Any])))
    TypedExpr.raw("VALUES ") |+| TypedExpr.joined(appliedRows, ", ")
  }

  /** Codec for decoding the produced rows when `Values` is consumed as a subquery that returns rows. */
  private[sharp] def codec: Codec[Row] = rowCodec(columns).asInstanceOf[Codec[Row]]

  /** Comma-joined, quoted column list — used after the alias when `Values` sits in FROM position. */
  private[sharp] def columnListSql: String =
    columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(c => s""""${c.name}"""").mkString(", ")

}

object Values {

  /**
   * Build a `Values` from one or more named-tuple rows. Every row shares the same named-tuple shape — field names and
   * types are taken from the first row's `Row` type parameter and the compile-time `DeriveColumns` resolution derives
   * codecs for each field. Rows widening to positional tuples (if the caller drops named-tuple syntax) is rejected —
   * the `Row <: NamedTuple.AnyNamedTuple` bound fires.
   *
   * {{{
   *   val lookup = Values.of(
   *     (id = 1, label = "alpha"),
   *     (id = 2, label = "beta")
   *   )
   *
   *   // As a subquery:
   *   users.insert.from(lookup)                      // (rejected: different shapes — illustrative)
   *
   *   // As a joinable relation:
   *   users.innerJoin(lookup.alias("l")).on(r => r.users.age ==== r.l.id).compile
   * }}}
   */
  def of[Row <: NamedTuple.AnyNamedTuple](row: Row, more: Row*)(using
    dc: DeriveColumns[NamedTuple.Names[Row], NamedTuple.DropNames[Row]]
  ): Values[dc.Out, Row] = {
    val cols = dc.value.asInstanceOf[dc.Out]
    val rs   = (row :: more.toList).map(_.asInstanceOf[Tuple].toList)
    new Values[dc.Out, Row](cols, rs)
  }

  /**
   * Build from any cats-`Reducible` non-empty container. Parallel to `InsertBuilder.values(rows: F[R])` — lets callers
   * feed a `NonEmptyList` / `NonEmptyVector` / `NonEmptyChain` directly without splatting.
   */
  def of[F[_]: Reducible, Row <: NamedTuple.AnyNamedTuple](rows: F[Row])(using
    dc: DeriveColumns[NamedTuple.Names[Row], NamedTuple.DropNames[Row]]
  ): Values[dc.Out, Row] = {
    val cols = dc.value.asInstanceOf[dc.Out]
    val rs   = Reducible[F].toNonEmptyList(rows).toList.map(_.asInstanceOf[Tuple].toList)
    new Values[dc.Out, Row](cols, rs)
  }

}
