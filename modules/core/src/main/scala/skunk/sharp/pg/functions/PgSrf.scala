package skunk.sharp.pg.functions

import skunk.{AppliedFragment, Codec}
import skunk.codec.all as pg
import skunk.sharp.*
import skunk.sharp.pg.{IsArray, PgTypeFor}

/**
 * Set-returning functions (`generate_series`, `unnest`, …) as joinable [[Relation]]s. The single-column shape below
 * fits the common cases: one function, one output column, always non-nullable (both `generate_series` and `unnest` emit
 * non-NULL rows for their declared ranges / array elements).
 *
 * Unlike Tables / Views, an SRF [[Relation]]'s default alias equals its output column name — Postgres auto-aliases an
 * un-`AS`ed SRF source to the function name, but using the column name here keeps the [[skunk.sharp.dsl.SelectBuilder]]
 * view's path `r.<colName>.<colName>` from repeating the noisy function name. `.alias("x")` re-aliases in the standard
 * way and flows through `Relation.fromFragmentWith(x)`, which we override to emit `func(args) AS "x"("col")` — Postgres
 * requires the explicit alias-and-column-list form when an SRF is renamed.
 *
 * Multi-column and composite-returning SRFs (`regexp_matches`, user-defined record-returning functions) are a later,
 * separate addition — they need a column tuple and are niche. This module covers the non-composite 80%.
 */
private[sharp] def srfRelation1[T, N <: String & Singleton](
  funcName: String,
  args: List[AppliedFragment],
  colName: N,
  codec0: Codec[T]
): Relation[Column[T, N, false, EmptyTuple] *: EmptyTuple] { type Alias = N; type Mode = AliasMode.Explicit } = {
  val col: Column[T, N, false, EmptyTuple] =
    Column[T, N, false, EmptyTuple](
      name = colName,
      tpe = skunk.sharp.pg.PgTypes.typeOf(codec0),
      codec = codec0,
      isNullable = false,
      attrs = Nil
    )
  val cols: Column[T, N, false, EmptyTuple] *: EmptyTuple = col *: EmptyTuple

  new Relation[Column[T, N, false, EmptyTuple] *: EmptyTuple] {
    type Alias = N
    type Mode  = AliasMode.Explicit
    val currentAlias: N                                        = colName
    val name: String                                           = colName
    val schema: Option[String]                                 = None
    val columns: Column[T, N, false, EmptyTuple] *: EmptyTuple = cols
    val expectedTableType: String                              = ""
    override def fromFragmentWith(x: String): AppliedFragment  = {
      val joinedArgs = TypedExpr.joined(args, ", ")
      TypedExpr.raw(s"$funcName(") |+| joinedArgs |+| TypedExpr.raw(s""") AS "$x"("$colName")""")
    }
  }
}

/**
 * `Pg.generateSeries` / `Pg.unnestAsRelation` — set-returning functions exposed as [[Relation]]s. Drop them into any
 * FROM / JOIN / LATERAL position:
 *
 * {{{
 *   // 1..10 as a relation — one column "n" of type int.
 *   Pg.generateSeries(1, 10).select
 *
 *   users.crossJoin(Pg.generateSeries(1, 3).alias("g"))
 *        .select(r => (r.users.email, r.g.n))
 *
 *   // Expand an array column into rows via LATERAL.
 *   users.innerJoinLateral(u => Pg.unnestAsRelation(u.tags).alias("t"))
 *        .on(_ => lit(true))
 *        .select(r => (r.users.email, r.t.v))
 * }}}
 */
trait PgSrf {

  /** `generate_series(start, stop)` — inclusive integer range, one column `n INT` per row. */
  def generateSeries(start: Int, stop: Int): Relation[Column[Int, "n", false, EmptyTuple] *: EmptyTuple] {
    type Alias = "n"
    type Mode  = AliasMode.Explicit
  } =
    srfRelation1[Int, "n"](
      "generate_series",
      List(typedExprToAf(Param.bind(start)), typedExprToAf(Param.bind(stop))),
      "n",
      pg.int4
    )

  /** `generate_series(start, stop, step)` — with an explicit step (positive or negative). */
  def generateSeries(start: Int, stop: Int, step: Int)
    : Relation[Column[Int, "n", false, EmptyTuple] *: EmptyTuple] { type Alias = "n"; type Mode = AliasMode.Explicit } =
    srfRelation1[Int, "n"](
      "generate_series",
      List(
        typedExprToAf(Param.bind(start)),
        typedExprToAf(Param.bind(stop)),
        typedExprToAf(Param.bind(step))
      ),
      "n",
      pg.int4
    )

  /**
   * `unnest(array)` as a [[Relation]]. Currently constrained to Args=Void inner expressions — typed-args
   * threading through SRF relations is roadmap.
   */
  def unnestAsRelation[A, E](a: TypedExpr[A, skunk.Void])(using
    @scala.annotation.unused ev: IsArray.Aux[A, E],
    pf: PgTypeFor[E]
  ): Relation[Column[E, "v", false, EmptyTuple] *: EmptyTuple] { type Alias = "v"; type Mode = AliasMode.Explicit } =
    srfRelation1[E, "v"](
      "unnest",
      List(typedExprToAf(a)),
      "v",
      pf.codec
    )

  private def typedExprToAf[T](e: TypedExpr[T, skunk.Void]): AppliedFragment =
    e.fragment.apply(skunk.Void)

}
