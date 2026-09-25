package skunk.sharp.pg.functions

import skunk.{AppliedFragment, Codec, Encoder, Fragment}
import skunk.codec.all as pg
import skunk.sharp.*
import skunk.sharp.dsl.{IsSrf, ProjArgsOf}
import skunk.sharp.internal.DeriveColumns
import skunk.sharp.pg.{IsArray, PgTypeFor}
import skunk.sharp.where.Where

import scala.NamedTuple

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
): TypedBodyRelation[Column[T, N, false, EmptyTuple] *: EmptyTuple, BA] {
  type Alias = N; type Mode = AliasMode.Explicit
} = {
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
     * Render the SRF as a single AppliedFragment for fallback paths (cache-warming, alias-wrapping). When the args
     * fragment has no typed parameters (`encoder.types.isEmpty`), bind args at Void inline. Typed-args SRFs (encoder
     * has types) can only be rendered via [[skunk.sharp.dsl.aliasedFromEntryParts]] in a SELECT/JOIN source position —
     * calling `fromFragmentWith` on them throws.
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

    /**
     * Disable the cached `starProj FROM` AppliedFragment — SRFs always carry args; the body rendering is handled by
     * `aliasedFromEntryParts` which threads typed args through Right slots.
     */
    override lazy val starProjFromAfOpt: Option[AppliedFragment] = None
  }
}

/** Element type of an array-ish parameter type — `Arr[E]` or a stdlib collection routed through it. */
type ArrayElem[A] = A match {
  case skunk.data.Arr[e] => e
  case List[e]           => e
  case Vector[e]         => e
  case Seq[e]            => e
}

/** Per-field element types of a tuple of array-typed expressions. */
type ArrayElems[T <: Tuple] <: Tuple = T match {
  case EmptyTuple              => EmptyTuple
  case TypedExpr[a, ?] *: tail => ArrayElem[a] *: ArrayElems[tail]
}

/**
 * Array codecs for each field type in `T` — one Postgres array parameter per column of a batch. Resolved at the call
 * site from the `PgTypeFor[Arr[E]]` instances.
 */
@scala.annotation.implicitNotFound(
  "skunk-sharp: no Postgres array codec for some field of ${T}. Array codecs come with `import skunk.sharp.dsl.given` " +
    "and cover the primitive element types (Option fields aren't supported in a batch)."
)
trait ArrayCodecs[T <: Tuple] {
  def codecs: List[Codec[skunk.data.Arr[Any]]]
}

object ArrayCodecs {

  given empty: ArrayCodecs[EmptyTuple] = new ArrayCodecs[EmptyTuple] { val codecs = Nil }

  given cons[H, T <: Tuple](using h: PgTypeFor[skunk.data.Arr[H]], t: ArrayCodecs[T]): ArrayCodecs[H *: T] =
    new ArrayCodecs[H *: T] {
      val codecs = h.codec.asInstanceOf[Codec[skunk.data.Arr[Any]]] :: t.codecs
    }

}

/**
 * Encoder for a whole batch as one value: `List[Row]` → one array per field, `$n, $n+1, …`. Splitting at encode time
 * means every array has exactly one element per row.
 */
private[sharp] def rowsAsArraysEncoder[Row](codecs: List[Codec[skunk.data.Arr[Any]]]): Encoder[List[Row]] =
  new Encoder[List[Row]] {
    override val types: List[skunk.data.Type]      = codecs.flatMap(_.types)
    override val sql: cats.data.State[Int, String] =
      cats.data.State { (n0: Int) =>
        codecs.zipWithIndex.foldLeft((n0, "")) { case ((n, acc), (c, i)) =>
          val (n1, s) = c.sql.run(n).value
          (n1, if (i == 0) s else s"$acc, $s")
        }
      }
    override def encode(rows: List[Row]): List[Option[skunk.data.Encoded]] = {
      val fields = rows.map(_.asInstanceOf[Product].productIterator.toList)
      codecs.zipWithIndex.flatMap { case (c, i) => c.encode(skunk.data.Arr.fromFoldable(fields.map(_(i)))) }
    }
  }

/** Wrap a multi-array args fragment so unequal array lengths fail fast with a clear error before hitting Postgres. */
private[sharp] def requireEqualLengths[A](frag: Fragment[A], project: A => List[Any]): Fragment[A] = {
  val inner = frag.encoder
  val enc   = new Encoder[A] {
    override val types: List[skunk.data.Type]                   = inner.types
    override val sql: cats.data.State[Int, String]              = inner.sql
    override def encode(a: A): List[Option[skunk.data.Encoded]] = {
      val lengths = project(a).collect {
        case xs: Iterable[?]        => xs.size
        case arr: skunk.data.Arr[?] => arr.flattenTo(List).size
      }
      if (lengths.distinct.sizeIs > 1)
        throw new IllegalArgumentException(
          s"skunk-sharp: unnest arrays must all have the same length (got ${lengths.mkString(", ")}); Postgres would " +
            "pad the shorter ones with NULL. Pg.unnestRows takes the rows as one List instead."
        )
      inner.encode(a)
    }
  }
  Fragment(frag.parts, enc, frag.origin)
}

/**
 * Multi-column SRF relation: `func(args) AS "alias"("c1", "c2", …)`. Columns come pre-built (codec per column); the
 * typed `argsFrag` threads into the outer query's Args like any typed-subquery body.
 */
private[sharp] def srfRelationN[Cols <: Tuple, BA](
  funcName: String,
  argsFrag: Fragment[BA],
  cols: Cols
): TypedBodyRelation[Cols, BA] { type Alias = "unnest"; type Mode = AliasMode.Explicit } = {
  val colNames = cols.toList.asInstanceOf[List[Column[?, ?, ?, ?]]].map(_.name)
  new TypedBodyRelation[Cols, BA] with IsSrf {
    type Alias = "unnest"
    type Mode  = AliasMode.Explicit
    val currentAlias: "unnest"         = "unnest"
    val name: String                   = "unnest"
    val schema: Option[String]         = None
    val columns: Cols                  = cols
    val expectedTableType: String      = ""
    val srfFuncName: String            = funcName
    val srfArgsFragment: Fragment[?]   = argsFrag
    val srfColumnName: String          = colNames.head
    override val srfColumnsSql: String = colNames.map(n => s""""$n"""").mkString(", ")

    override def fromFragmentWith(x: String): AppliedFragment =
      throw new UnsupportedOperationException(
        s"skunk-sharp: multi-column '$funcName' can only be rendered in a FROM / JOIN / USING source position"
      )

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
    start: TypedExpr[Int, A],
    stop: TypedExpr[Int, B]
  ): TypedBodyRelation[Column[Int, "n", false, EmptyTuple] *: EmptyTuple, Where.Concat[A, B]] {
    type Alias = "n"
    type Mode  = AliasMode.Explicit
  } = {
    val argsFrag: Fragment[Where.Concat[A, B]] = TypedExpr.combineSepInl[A, B](start.fragment, ", ", stop.fragment)
    srfRelation1[Int, "n", Where.Concat[A, B]]("generate_series", argsFrag, "n", pg.int4)
  }

  /** `generate_series(start, stop, step)` — with an explicit step (positive or negative). */
  inline def generateSeries[A, B, C](
    start: TypedExpr[Int, A],
    stop: TypedExpr[Int, B],
    step: TypedExpr[Int, C]
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
   * `Param[Arr[E]]` for a deferred array, `lit(arr)` for a compile-time literal, or any other `TypedExpr[Arr[E], A]`.
   */
  def unnestAsRelation[A, E, BA](a: TypedExpr[A, BA])(using
    @scala.annotation.unused ev: IsArray.Aux[A, E],
    pf: PgTypeFor[E]
  ): TypedBodyRelation[Column[E, "v", false, EmptyTuple] *: EmptyTuple, BA] {
    type Alias = "v"
    type Mode  = AliasMode.Explicit
  } =
    srfRelation1[E, "v", BA]("unnest", a.fragment, "v", pf.codec)

  /**
   * `unnest(a1, a2, …) AS "unnest"("n1", "n2", …)` — zip several arrays into rows, one column per named-tuple field.
   * The standard way to feed a whole batch of rows through **one** prepared statement: pass `Param[List[T]]` per column
   * and bind the lists at execute time.
   *
   * {{{
   *   Pg.unnestAsRelation((name = Param[List[String]], qty = Param[List[Int]])).alias("incoming")
   *   // → unnest($1, $2) AS "incoming"("name", "qty"), Args = (List[String], List[Int])
   * }}}
   *
   * The arrays must have the same length (checked when the statement is encoded: Postgres would pad shorter ones with
   * NULL). To make that unrepresentable, pass the rows themselves with [[unnestRows]].
   */
  inline def unnestAsRelation[R <: NamedTuple.AnyNamedTuple, TOut](arrays: R)(using
    dc: DeriveColumns[NamedTuple.Names[R], ArrayElems[NamedTuple.DropNames[R]]],
    pa: ProjArgsOf.Aux[NamedTuple.DropNames[R], TOut]
  ): TypedBodyRelation[dc.Out, TOut] { type Alias = "unnest"; type Mode = AliasMode.Explicit } = {
    val exprs = arrays.asInstanceOf[Tuple].toList.asInstanceOf[List[TypedExpr[?, ?]]]
    val args  = TypedExpr.combineList[TOut](exprs.map(_.fragment), ", ", (a: TOut) => pa.project(a))
    srfRelationN[dc.Out, TOut](
      "unnest",
      requireEqualLengths(args, (a: TOut) => pa.project(a)),
      dc.value.asInstanceOf[dc.Out]
    )
  }

  /**
   * A whole batch of rows as **one** typed parameter: `unnest($1, $2, …) AS "unnest"("f1", "f2", …)`, where the
   * `List[Row]` bound at execute time is split into one array per field. `Row` is a case class or a named tuple; its
   * field names become the columns. Every array gets exactly one element per row, so lengths can't disagree.
   *
   * {{{
   *   case class Sync(name: String, capacity: Int)
   *   Pg.unnestRows[Sync].alias("incoming")   // Args = List[Sync]
   * }}}
   *
   * Needs `import skunk.sharp.dsl.given` for the array codecs.
   */
  inline def unnestRows[Row](using
    dc: DeriveColumns[NamedTuple.Names[NamedTuple.From[Row]], NamedTuple.DropNames[NamedTuple.From[Row]]],
    ac: ArrayCodecs[NamedTuple.DropNames[NamedTuple.From[Row]]]
  ): TypedBodyRelation[dc.Out, List[Row]] { type Alias = "unnest"; type Mode = AliasMode.Explicit } = {
    val enc  = rowsAsArraysEncoder[Row](ac.codecs)
    val frag = Fragment[List[Row]](List(Right(enc.sql)), enc, skunk.util.Origin.unknown)
    srfRelationN[dc.Out, List[Row]]("unnest", frag, dc.value.asInstanceOf[dc.Out])
  }

}
