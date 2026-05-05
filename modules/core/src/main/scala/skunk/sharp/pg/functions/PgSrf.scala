package skunk.sharp.pg.functions

import skunk.{AppliedFragment, Codec, Fragment}
import skunk.codec.all as pg
import skunk.sharp.*
import skunk.sharp.dsl.IsSrf
import skunk.sharp.pg.{IsArray, PgTypeFor}
import skunk.sharp.where.Where

/**
 * Set-returning functions (`generate_series`, `unnest`, …) as joinable [[Relation]]s. The single-column shape below
 * fits the common cases: one function, one output column, always non-nullable (both `generate_series` and `unnest` emit
 * non-NULL rows for their declared ranges / array elements).
 *
 * Unlike Tables / Views, an SRF [[Relation]]'s default alias equals its output column name — Postgres auto-aliases an
 * un-`AS`ed SRF source to the function name, but using the column name here keeps the [[skunk.sharp.dsl.SelectBuilder]]
 * view's path `r.<colName>.<colName>` from repeating the noisy function name. `.alias("x")` re-aliases in the standard
 * way; the renamed shape `func(args) AS "x"("col")` is rendered by [[skunk.sharp.dsl.aliasedFromEntryParts]] via the
 * [[IsSrf]] marker.
 *
 * `Param`s in the SRF args (e.g. `Pg.generateSeries(Param[Int], Param[Int])`) thread into the outer query's args via
 * the same per-source `BodyArgs` plumbing as typed-subquery aliases.
 *
 * Multi-column and composite-returning SRFs (`regexp_matches`, user-defined record-returning functions) are a later,
 * separate addition — they need a column tuple and are niche. This module covers the non-composite 80%.
 */
private[sharp] def srfRelation1[T, N <: String & Singleton, BA](
  funcName: String,
  argsFrag: Fragment[BA],
  colName: N,
  codec0: Codec[T]
): TypedBodyRelation[Column[T, N, false, EmptyTuple] *: EmptyTuple, BA] { type Alias = N; type Mode = AliasMode.Explicit } = {
  val col: Column[T, N, false, EmptyTuple] =
    Column[T, N, false, EmptyTuple](
      name = colName,
      tpe = skunk.sharp.pg.PgTypes.typeOf(codec0),
      codec = codec0,
      isNullable = false,
      attrs = Nil
    )
  val cols: Column[T, N, false, EmptyTuple] *: EmptyTuple = col *: EmptyTuple

  new TypedBodyRelation[Column[T, N, false, EmptyTuple] *: EmptyTuple, BA] with IsSrf {
    type Alias = N
    type Mode  = AliasMode.Explicit
    val currentAlias: N                                        = colName
    val name: String                                           = colName
    val schema: Option[String]                                 = None
    val columns: Column[T, N, false, EmptyTuple] *: EmptyTuple = cols
    val expectedTableType: String                              = ""
    val srfFuncName: String                                    = funcName
    val srfArgsFragment: Fragment[?]                           = argsFrag
    val srfColumnName: String                                  = colName

    /**
     * Render the SRF as a single AppliedFragment for fallback paths (cache-warming, alias-wrapping). When the
     * args fragment has no typed parameters (`encoder.types.isEmpty`), bind args at Void inline. Typed-args
     * SRFs (encoder has types) can only be rendered via [[skunk.sharp.dsl.aliasedFromEntryParts]] in a
     * SELECT/JOIN source position — calling `fromFragmentWith` on them throws.
     */
    override def fromFragmentWith(x: String): AppliedFragment =
      if (argsFrag.encoder.types.isEmpty) {
        val argsAf = argsFrag.asInstanceOf[Fragment[skunk.Void]].apply(skunk.Void)
        TypedExpr.raw(s"$funcName(") |+| argsAf |+| TypedExpr.raw(s""") AS "$x"("$colName")""")
      } else
        throw new UnsupportedOperationException(
          s"skunk-sharp: SRF '$funcName' has typed args (Param[T] or other typed expressions) and can only be " +
          s"rendered via a SELECT/JOIN source position (aliasedFromEntryParts). The fallback rendering path " +
          s"(`fromFragmentWith` / `starProjFromAfOpt`) does not support typed args."
        )

    /** Disable the cached `starProj FROM` AppliedFragment — SRFs always carry args; the body rendering is handled
      * by `aliasedFromEntryParts` which threads typed args through Right slots. */
    override lazy val starProjFromAfOpt: Option[AppliedFragment] = None
  }
}

/**
 * `Pg.generateSeries` / `Pg.unnestAsRelation` — set-returning functions exposed as [[Relation]]s. Drop them into any
 * FROM / JOIN / LATERAL position:
 *
 * {{{
 *   // 1..10 as a relation — one column "n" of type int.
 *   Pg.generateSeries(lit(1), lit(10)).select
 *
 *   // Param-bearing range: typed Args threads into outer compile.
 *   Pg.generateSeries(Param[Int], Param[Int]).select  // QueryTemplate[(Int, Int), …]
 *
 *   users.crossJoin(Pg.generateSeries(lit(1), lit(3)).alias("g"))
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
  inline def generateSeries[A, B](
    start: TypedExpr[Int, A], stop: TypedExpr[Int, B]
  ): TypedBodyRelation[Column[Int, "n", false, EmptyTuple] *: EmptyTuple, Where.Concat[A, B]] {
    type Alias = "n"
    type Mode  = AliasMode.Explicit
  } = {
    val argsFrag: Fragment[Where.Concat[A, B]] = TypedExpr.combineSepInl[A, B](start.fragment, ", ", stop.fragment)
    srfRelation1[Int, "n", Where.Concat[A, B]]("generate_series", argsFrag, "n", pg.int4)
  }

  /** `generate_series(start, stop, step)` — with an explicit step (positive or negative). */
  inline def generateSeries[A, B, C](
    start: TypedExpr[Int, A], stop: TypedExpr[Int, B], step: TypedExpr[Int, C]
  ): TypedBodyRelation[Column[Int, "n", false, EmptyTuple] *: EmptyTuple, Where.Concat[Where.Concat[A, B], C]] {
    type Alias = "n"
    type Mode  = AliasMode.Explicit
  } = {
    val ab: Fragment[Where.Concat[A, B]] = TypedExpr.combineSepInl[A, B](start.fragment, ", ", stop.fragment)
    val abc: Fragment[Where.Concat[Where.Concat[A, B], C]] =
      TypedExpr.combineSepInl[Where.Concat[A, B], C](ab, ", ", step.fragment)
    srfRelation1[Int, "n", Where.Concat[Where.Concat[A, B], C]]("generate_series", abc, "n", pg.int4)
  }

  /**
   * `unnest(array)` as a [[Relation]]. The array expression's typed Args thread into the outer query: pass
   * `Param[Arr[E]]` for a deferred array, `lit(arr)` for a compile-time literal, or any other
   * `TypedExpr[Arr[E], A]`.
   */
  def unnestAsRelation[A, E, BA](a: TypedExpr[A, BA])(using
    @scala.annotation.unused ev: IsArray.Aux[A, E],
    pf: PgTypeFor[E]
  ): TypedBodyRelation[Column[E, "v", false, EmptyTuple] *: EmptyTuple, BA] {
    type Alias = "v"
    type Mode  = AliasMode.Explicit
  } =
    srfRelation1[E, "v", BA]("unnest", a.fragment, "v", pf.codec)

}
