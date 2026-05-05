package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.where.Where

import scala.NamedTuple

/**
 * N-source SQL joins. Every source is a [[Relation]]. Tables and views default their alias to their own name
 * (`users.innerJoin(posts)` just works). To rename a relation for FROM-position use, or to promote a subquery / set-op
 * / VALUES into a joinable source, call `.alias("name")` — the single verb for "give this thing a FROM-scope identity".
 *
 * The lambda passed to `.where` / `.select` / `.orderBy` / `.groupBy` / `.having` receives a `JoinedView[Sources]` — a
 * Scala 3 named tuple keyed by each source's `Alias` singleton — so columns are reached as `r.<alias>.<column>`.
 *
 * {{{
 *   users
 *     .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
 *     .leftJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
 *     .select(r => (r.users.email, r.posts.title, r.tags.name))
 *     .where(r => r.users.age >= 18)
 *     .compile
 * }}}
 *
 * LEFT JOIN flips the right-side columns' nullability for `.where` / `.select` — value types become `Option[T]`, codecs
 * are `.opt`. The `.on` predicate still sees the declared (non-nullable) types — Postgres evaluates `ON` before
 * NULL-padding unmatched rows.
 */

// ---- .alias extension on Relation --------------------------------------------------------------

/**
 * `.alias("u")` on any relation — re-alias it for FROM-position use. Same relation kind, same rendering kernel
 * (`fromFragmentWith`), but a different `Alias` singleton. Enables self-joins
 * (`users.alias("u1").innerJoin(users.alias("u2"))`) and renaming a view / subquery from whatever its default was.
 *
 * Named `.alias` (not `.as`) to keep clean separation from [[skunk.sharp.TypedExpr.as]], which renames a *projected
 * column* — a different operation.
 *
 * The returned `Relation[Cols] { type Alias = A }` is the sole representation: no separate wrapper type in the public
 * vocabulary. The internal implementation forwards every member except `alias` to the underlying relation.
 */
extension [Cols <: Tuple](r: Relation[Cols]) {

  def alias[A <: String & Singleton](a: A): TypedBodyRelation[Cols, skunk.Void] { type Alias = A; type Mode = AliasMode.Explicit } = {
    val underlying = r
    val newAlias   = a
    new TypedBodyRelation[Cols, skunk.Void] {
      type Alias = A
      type Mode  = AliasMode.Explicit
      val currentAlias: A                 = newAlias
      def name: String                    = underlying.name
      def columns: Cols                   = underlying.columns
      def schema: Option[String]          = underlying.schema
      def expectedTableType: String       = underlying.expectedTableType
      override def hasFromClause: Boolean = underlying.hasFromClause
      override def qualifiedName: String  = underlying.qualifiedName
      override def fromFragmentWith(x: String): AppliedFragment = underlying.fromFragmentWith(x)
    }
  }

}

/**
 * `.alias("name")` on a single-source whole-row [[SelectBuilder]] — promote the query to a joinable [[Relation]] in a
 * FROM position, threading any typed `Args` from the inner query's WHERE / GROUP BY / HAVING into the resulting
 * relation's `BodyArgs`.
 *
 * The inner SQL is held **lazily** as a typed `Fragment[CombinedInnerArgs]` — no parameters are applied until the outer
 * query's `.compile` walks this source. One terminal `.compile` for the whole tree.
 *
 * {{{
 *   // Static subquery: no Params inside, BodyArgs = Void
 *   val active = users.select.where(u => u.deleted_at.isNull).alias("active")
 *   active.innerJoin(orders).on(r => r.active.id ==== r.orders.user_id).compile
 *
 *   // Typed subquery: Param inside; BodyArgs = UUID flows into outer Args
 *   val byId = users.select.where(u => u.id === Param[UUID]).alias("u")
 *   byId.select.compile  // : QueryTemplate[UUID, NamedRow]
 * }}}
 */
extension [Ss <: Tuple, GroupsT <: Tuple, WA, HA](sb: SelectBuilder[Ss, GroupsT, WA, HA]) {

  def alias[A <: String & Singleton, SArgs, GArgs](a: A)(using
    ev:   IsSingleSource[Ss],
    sbOf: SourceBodyArgsOf.Aux[Ss, SArgs],
    g:    ProjArgsOf.Aux[GroupsT, GArgs],
    cs:   Where.Concat2[SArgs, WA],
    csg:  Where.Concat2[Where.Concat[SArgs, WA], GArgs],
    csgh: Where.Concat2[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]
  ): TypedBodyRelation[ev.Cols, Where.Concat[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]] {
    type Alias    = A
    type Mode     = AliasMode.Explicit
    type BodyArgs = Where.Concat[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]
  } = {
    type CombinedArgs = Where.Concat[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]
    val newAlias  = a
    val cols      = sb.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].head.effectiveCols.asInstanceOf[ev.Cols]
    val innerFrag: Fragment[CombinedArgs] =
      sb.compileBodyFragment[SArgs, GArgs](using ev, sbOf, g, cs, csg, csgh)
    new TypedBodyRelation[ev.Cols, CombinedArgs] {
      type Alias = A
      type Mode  = AliasMode.Explicit
      override def bodyFragmentOpt: Option[Fragment[?]]            = Some(innerFrag)
      override lazy val starProjFromAfOpt: Option[AppliedFragment] = None
      val currentAlias: A           = newAlias
      val name: String              = newAlias
      val schema: Option[String]    = None
      val columns: ev.Cols          = cols
      val expectedTableType: String = ""
      override def fromFragmentWith(x: String): AppliedFragment =
        throw new UnsupportedOperationException(
          "skunk-sharp internal: fromFragmentWith called on a typed subquery relation — aliasedFromEntryParts should handle this"
        )
    }
  }

}

/**
 * `.alias("sub")` on a [[ProjectedSelect]] — promote a column-list SELECT to a joinable [[Relation]]. Every element of
 * the projection must have a stable name (a bare column reference or `expr.as("name")`); arbitrary unaliased
 * expressions (`Pg.lower(u.email)`, `u.email ++ u.suffix`) have no name Postgres is obliged to use, so they're rejected
 * at compile time by [[AllNamedProj]] with a pointed error that names the fix.
 *
 * Enables projected subquery-as-relation and projected `LATERAL` sources — both ubiquitous for per-row-parameterised
 * queries. The outer relation's `Cols` tuple is computed by [[ProjCols]] from the projection shape, so
 * `r.<alias>.<col>` resolves at the outer site.
 *
 * A terminal `.compile` on the `ProjectedSelect` runs lazily when the outer query's own `.compile` walks this source —
 * there's still only one user-visible `.compile` on the whole tree. `GroupCoverage` is summoned here so the inner
 * rendering is as valid as a standalone `.compile` would be.
 */
/**
 * `.alias("sub")` on a [[ProjectedSelect]] — promote a column-list SELECT to a joinable [[Relation]], threading any
 * typed `Args` from the inner query's WHERE / GROUP BY / HAVING / ORDER BY into the resulting relation's `BodyArgs`.
 *
 * Every column in the projection must carry a name — either a bare [[TypedColumn]] or an [[AliasedExpr]] — so the
 * derived relation's column list is well-defined.
 */
extension [Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, DA <: Tuple, OA <: Tuple, WA, HA, Row](
  ps: ProjectedSelect[Ss, Proj, Groups, DA, OA, WA, HA, Row]
)(using
  gc:  GroupCoverage[Proj, Groups],
  @scala.annotation.unused np: AllNamedProj[Proj]
) {

  // Concat-chain evidences (left-fold over slot order):
  // DA2 ⊕ PA ⊕ SA ⊕ OnA ⊕ WA ⊕ GA ⊕ HA ⊕ OA2 — each name spells the accumulator at that step.
  def alias[A <: String & Singleton, SA, OnA, DA2, PA, GA, OA2](a: A)(using
    sbOf:     SourceBodyArgsOf.Aux[Ss, SA],
    bff:      SourceBodyArgsProj[Ss],
    onSum:    SourceOnArgsOf.Aux[Ss, OnA],
    onProj:   SourceOnArgsProj[Ss],
    d:        ProjArgsOf.Aux[DA, DA2],
    pa:       ProjArgsOf.Aux[Proj, PA],
    g:        ProjArgsOf.Aux[Groups, GA],
    o:        ProjArgsOf.Aux[OA, OA2],
    dp:       Where.Concat2[DA2, PA],
    dps:      Where.Concat2[Where.Concat[DA2, PA], SA],
    dpso:     Where.Concat2[Where.Concat[Where.Concat[DA2, PA], SA], OnA],
    dpsow:    Where.Concat2[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA],
    dpsowg:   Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA],
    dpsowgh:  Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA],
    dpsowgho: Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]
  ): TypedBodyRelation[ProjCols[Proj], Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]] {
    type Alias    = A
    type Mode     = AliasMode.Explicit
    type BodyArgs = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]
  } = {
    type CombinedArgs = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]
    val newAlias = a
    val cols     = buildProjectedCols(ps.projections).asInstanceOf[ProjCols[Proj]]
    val innerFrag: Fragment[CombinedArgs] =
      ps.compileBodyFragment[SA, OnA, DA2, PA, GA, OA2](
        using gc, sbOf, bff, onSum, onProj, d, pa, g, o, dp, dps, dpso, dpsow, dpsowg, dpsowgh, dpsowgho
      )
    new TypedBodyRelation[ProjCols[Proj], CombinedArgs] {
      type Alias = A
      type Mode  = AliasMode.Explicit
      override def bodyFragmentOpt: Option[Fragment[?]]            = Some(innerFrag)
      override lazy val starProjFromAfOpt: Option[AppliedFragment] = None
      val currentAlias: A           = newAlias
      val name: String              = newAlias
      val schema: Option[String]    = None
      val columns: ProjCols[Proj]   = cols
      val expectedTableType: String = ""
      override def fromFragmentWith(x: String): AppliedFragment =
        throw new UnsupportedOperationException(
          "skunk-sharp internal: fromFragmentWith called on a typed subquery relation — aliasedFromEntryParts should handle this"
        )
    }
  }

}

/**
 * Reconstruct a runtime column tuple from a projection list. Each element is either a [[TypedColumn]] (column name +
 * codec + compile-time nullability, now threaded through `.nullable`) or an [[AliasedExpr]] (alias name + codec, always
 * treated as non-nullable — any expression that could produce NULL is the caller's responsibility to wrap in an
 * explicit `Option[_]` codec). Third shapes are rejected at compile time by [[AllNamedProj]]; the default arm here
 * throws only because pattern exhaustiveness on `TypedExpr[?]` is open.
 */
private[sharp] def buildProjectedCols(projections: List[TypedExpr[?, ?]]): Tuple = {
  val cols = projections.map {
    case tc: TypedColumn[?, ?, ?] =>
      Column[Any, "x", Boolean, Tuple](
        name = tc.name.asInstanceOf["x"],
        tpe = skunk.sharp.pg.PgTypes.typeOf(tc.codec.asInstanceOf[Codec[Any]]),
        codec = tc.codec.asInstanceOf[Codec[Any]],
        isNullable = tc.nullable.asInstanceOf[Boolean],
        attrs = Nil
      )
    case ae: AliasedExpr[?, ?, ?] =>
      Column[Any, "x", Boolean, Tuple](
        name = ae.aliasName.asInstanceOf["x"],
        tpe = skunk.sharp.pg.PgTypes.typeOf(ae.codec.asInstanceOf[Codec[Any]]),
        codec = ae.codec.asInstanceOf[Codec[Any]],
        isNullable = false,
        attrs = Nil
      )
    case other =>
      throw new IllegalStateException(
        s"skunk-sharp: projection element is neither a TypedColumn nor an AliasedExpr — should have been rejected at compile time: $other"
      )
  }
  Tuple.fromArray(cols.toArray[Any])
}

/**
 * `.alias("v")` on a [[Values]] — promote the literal row source to a joinable [[Relation]]. Every relation extension
 * (`.innerJoin` / `.leftJoin` / `.select` / …) works on the result. The rendered FROM fragment is
 * `(VALUES (…), (…)) AS "alias" ("col1", "col2", …)` — Postgres requires a derived `VALUES` to declare both its alias
 * and its column list.
 *
 * Lives here (not in `Values.scala`) because Scala 3 requires overloaded extensions to share a single top-level
 * definition group; this `alias` sits alongside the `Relation` and `SelectBuilder` overloads.
 */
extension [Cols <: Tuple, Row <: scala.NamedTuple.AnyNamedTuple](v: Values[Cols, Row]) {

  def alias[A <: String & Singleton](a: A): TypedBodyRelation[Cols, skunk.Void] {
    type Alias = A
    type Mode  = AliasMode.Explicit
  } = {
    val newAlias                           = a
    val colsVal                            = v.columns
    val colsList                           = v.columnListSql
    val renderInner: () => AppliedFragment = () => v.render
    new TypedBodyRelation[Cols, skunk.Void] {
      type Alias = A
      type Mode  = AliasMode.Explicit
      val currentAlias: A                                       = newAlias
      val name: String                                          = newAlias
      val schema: Option[String]                                = None
      val columns: Cols                                         = colsVal
      val expectedTableType: String                             = ""
      override def fromFragmentWith(x: String): AppliedFragment =
        TypedExpr.raw("(") |+| renderInner() |+| TypedExpr.raw(s""") AS "$x" ($colsList)""")
    }
  }

}

/**
 * `.alias("x")` on a [[CteRelation]] — re-alias it while preserving the CTE identity so [[collectCtesInOrder]] can
 * detect it and emit the WITH preamble regardless of how many alias layers are applied. Useful for CTE self-joins:
 * `c.alias("a").innerJoin(c.alias("b")).on(...)`.
 *
 * Lives here because Scala 3 requires overloaded extensions to share a single top-level definition group.
 */
extension [Cols <: Tuple, Name <: String & Singleton, BA](c: CteRelation[Cols, Name, BA]) {

  def alias[A <: String & Singleton](a: A): (TypedBodyRelation[Cols, skunk.Void] & IsCte) {
    type Alias = A
    type Mode  = AliasMode.Explicit
  } = {
    val cteRef = c
    new TypedBodyRelation[Cols, skunk.Void] with IsCte {
      type Alias = A
      type Mode  = AliasMode.Explicit
      val currentAlias: A                                       = a
      def name: String                                          = cteRef.cteName
      def columns: Cols                                         = cteRef.cols0
      def schema: Option[String]                                = None
      def expectedTableType: String                             = ""
      def underlyingCte                                         = cteRef
      override def fromFragmentWith(x: String): AppliedFragment = cteRef.fromFragmentWith(x)
    }
  }

}

// ---- JoinKind + NullableCols -------------------------------------------------------------------

/**
 * Kind of join — drives the rendered keyword and whether either side's columns get nullabilified:
 *   - `Inner` / `Cross`: neither side nullabilified (no null-padding).
 *   - `Left`: right-side cols become `Option[_]` (right rows may be absent).
 *   - `Right`: left-side cols become `Option[_]` (left rows may be absent) — mirror of `Left`.
 *   - `Full`: both sides become `Option[_]` (either side may be absent).
 */
enum JoinKind(val sql: String) {
  case Inner extends JoinKind("INNER JOIN")
  case Left  extends JoinKind("LEFT JOIN")
  case Right extends JoinKind("RIGHT JOIN")
  case Full  extends JoinKind("FULL OUTER JOIN")
  case Cross extends JoinKind("CROSS JOIN")

  /**
   * Pre-rendered ` <kind> ` and ` <kind> LATERAL ` AppliedFragments — interned once per enum case so the SELECT
   * compiler can splice the join keyword as a single shared instance instead of `s"…"`-interpolating + allocating
   * a fresh `Fragment` + `AppliedFragment` per source per compile.
   */
  val keywordAf:        skunk.AppliedFragment = skunk.sharp.internal.RawConstants.intern(s" $sql ")
  val lateralKeywordAf: skunk.AppliedFragment = skunk.sharp.internal.RawConstants.intern(s" $sql LATERAL ")
}

/**
 * Type-level "make one column nullable" — idempotent per element. Gated on the `Null` phantom: already-nullable columns
 * (phantom `true`) pass through unchanged, so chaining outer joins that would re-nullabilify the same source doesn't
 * produce `Option[Option[T]]` at the Scala level. Non-nullable columns (phantom `false`) have their value type wrapped
 * in `Option` and the phantom flipped to `true`.
 */
type NullableCol[C] = C match {
  case Column[t, n, true, attrs]  => Column[t, n, true, attrs]
  case Column[t, n, false, attrs] => Column[Option[t], n, true, attrs]
}

/** Type-level "make every column nullable". Walks the tuple and applies [[NullableCol]] per element. */
type NullableCols[Cols <: Tuple] <: Tuple = Cols match {
  case EmptyTuple   => EmptyTuple
  case head *: tail => NullableCol[head] *: NullableCols[tail]
}

/**
 * Runtime counterpart to [[NullableCols]]: walk the columns tuple and wrap each codec in `.opt` so the decoder emits
 * `Option[T]`. Name / tpe / attrs are preserved. Idempotent — columns whose `isNullable` is already `true` are returned
 * unchanged so re-wrapping (chained outer joins) doesn't produce `Option[Option[T]]`.
 */
private[sharp] def nullabilifyCols(cols: Tuple): Tuple = {
  val wrapped = cols.toList.map {
    case c: Column[?, ?, ?, ?] if c.isNullable => c
    case c: Column[?, ?, ?, ?]                 =>
      Column[Any, "x", Boolean, Tuple](
        name = c.name.asInstanceOf["x"],
        tpe = c.tpe,
        codec = c.codec.asInstanceOf[Codec[Any]].opt.asInstanceOf[Codec[Any]],
        isNullable = true,
        attrs = c.attrs
      )
    case other =>
      other
  }
  Tuple.fromArray(wrapped.toArray[Any])
}

/**
 * Walk a tuple of [[SourceEntry]] and rebuild each one with its effective cols nullabilified. Used by RIGHT / FULL JOIN
 * to mark every already-committed source's columns as possibly-absent: a RIGHT JOIN against the chain so far means
 * *any* of the earlier sources may be NULL-padded when the right side dominates.
 *
 * Paired with the type-level [[NullabilifySources]] match type so the SelectBuilder the user transitions into sees the
 * flipped column types in its `.where` / `.select` views.
 */
type NullabilifySources[Ss <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                            => EmptyTuple
  case SourceEntry[r, c0, c, a, oa] *: tail =>
    SourceEntry[r, c0, NullableCols[c], a, oa] *: NullabilifySources[tail]
}

private[sharp] def nullabilifySources(sources: Tuple): Tuple = {
  val wrapped = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].map { s =>
    new SourceEntry[Relation[Tuple], Tuple, Tuple, "x", Any](
      s.relation.asInstanceOf[Relation[Tuple]],
      s.alias.asInstanceOf["x"],
      s.originalCols,
      nullabilifyCols(s.effectiveCols),
      s.kind,
      s.onPredOpt.asInstanceOf[Option[Where[Any]]]
    )
  }
  Tuple.fromArray(wrapped.toArray[Any])
}

// ---- AsRelation: identity-plus-type-extraction -------------------------------------------------

/**
 * Extract `(Relation, Cols, Alias)` from any value that's already a `Relation` with a refined `Alias` type member.
 * After the unification, this is just identity with type-level destructuring — there's no conversion to perform, only
 * the three phantoms surfaced as Aux type members so the join / select machinery can forward them.
 *
 * Users never call this directly; it's summoned by the `innerJoin` / `leftJoin` / `crossJoin` / `.select` extensions.
 * The `Alias` singleton flows into `SourceEntry` where `JoinedView` uses it as a `NamedTuple` label.
 */
sealed trait AsRelation[T] {
  type Rel <: Relation[Cols]
  type Cols <: Tuple
  type Alias <: String & Singleton
  type Mode <: AliasMode
  def apply(t: T): Rel

  /**
   * Retrieve the alias as an `Alias`-typed value. Exists because path-dependent access `rel.currentAlias` from a caller
   * scope where `Rel` is an abstract type parameter doesn't auto-reduce to `Alias` — Scala won't collapse the
   * refinement `Rel { type Alias = Alias }` at use sites. The typeclass witnesses the equality once, here, and hands
   * the value back at the refined type so call sites don't need `.asInstanceOf`.
   */
  def aliasValue(t: T): Alias
}

object AsRelation {

  type Aux[T, R0 <: Relation[C0], C0 <: Tuple, A <: String & Singleton, M <: AliasMode] = AsRelation[T] {
    type Rel   = R0
    type Cols  = C0
    type Alias = A
    type Mode  = M
  }

  /**
   * The only given — any `Relation[CC]` with a known `Alias = A` and `Mode = M` satisfies the contract. Table / View
   * (Mode = Implicit), `.alias("x")`-wrapped relations (Mode = Explicit), and the subquery relation produced by
   * `sb.alias("x")` (also Mode = Explicit) all flow through this single instance.
   */
  given fromRelation[
    R <: Relation[CC] { type Alias = A; type Mode = M },
    CC <: Tuple,
    A <: String & Singleton,
    M <: AliasMode
  ]: AsRelation.Aux[R, R, CC, A, M] = new AsRelation[R] {
    type Rel   = R
    type Cols  = CC
    type Alias = A
    type Mode  = M
    def apply(r: R): R      = r
    def aliasValue(r: R): A = r.currentAlias
  }

}

// ---- AliasNotUsed: compile-time distinct-alias check for JOIN --------------------------------

/**
 * Evidence that the singleton alias `A` is **not** already among the already-committed source aliases in `Ss`. Summoned
 * by every `innerJoin` / `leftJoin` / `crossJoin` so a self-join like `users.innerJoin(users)` — where both sides
 * default to the implicit alias `"users"` — is rejected at compile time with a pointed error. Postgres would reject it
 * at runtime anyway; this surfaces the problem at build time and points at the fix.
 *
 * The recursion walks `Ss` term-by-term, demanding `NotGiven[A =:= head]` at each step. Singleton alias types erase to
 * distinct references, so `=:=` between two different singletons has no instance and `NotGiven` succeeds; two
 * occurrences of the same singleton reduce to the identity `=:=` and `NotGiven` fails.
 *
 * To self-join legitimately, supply explicit aliases on at least one side:
 * `users.alias("u1").innerJoin(users.alias("u2")).on(r => r.u1.id ==== r.u2.id)`.
 */
@scala.annotation.implicitNotFound(
  "Relation alias `${A}` is already in use by another source in this query. Use `.alias(\"…\")` to give this source a distinct alias (e.g. for a self-join: `users.alias(\"u1\").innerJoin(users.alias(\"u2\"))`)."
)
sealed trait AliasNotUsed[A <: String & Singleton, Ss <: Tuple]

object AliasNotUsed {

  given atEmpty[A <: String & Singleton]: AliasNotUsed[A, EmptyTuple] =
    new AliasNotUsed[A, EmptyTuple] {}

  given atCons[A <: String & Singleton, H <: String & Singleton, T <: Tuple](using
    scala.util.NotGiven[A =:= H],
    AliasNotUsed[A, T]
  ): AliasNotUsed[A, H *: T] = new AliasNotUsed[A, H *: T] {}

}

// ---- SourceEntry + match types over Sources ----------------------------------------------------

/**
 * A single source in an N-source join. Carries both the declared column tuple (`Cols0`) and the effective one (`Cols` =
 * `NullableCols[Cols0]` for LEFT-joined sources). The `.where` / `.select` lambda sees `Cols`; the `.on` lambda for the
 * just-added source sees `Cols0`.
 *
 * `Alias` is duplicated here (also lives on `relation.alias`) because the match types below pattern-match against the
 * `*:` arm of a tuple to extract type parameters positionally — pulling the alias out of a path-dependent type through
 * a match type is not reliable. The value-level redundancy is negligible.
 */
final class SourceEntry[
  R <: Relation[Cols0],
  Cols0 <: Tuple,
  Cols <: Tuple,
  Alias <: String & Singleton,
  OnArgs
] private[sharp] (
  val relation: R,
  val alias: Alias,
  val originalCols: Cols0,
  val effectiveCols: Cols,
  val kind: JoinKind,                   // for the base source this is `Inner` (cosmetic — not rendered for index 0)
  val onPredOpt: Option[Where[OnArgs]], // `None` for the base source and CROSS joins; `Some` for INNER/LEFT/RIGHT/FULL
  val isLateral: Boolean = false
)

/**
 * Aliases tuple: one alias literal per committed source. Pattern-matches `SourceEntry[?, ?, ?, a, ?]` directly in the `*:`
 * arm — going through a separate `AliasOf` helper breaks reduction when `Ss` is constructed through multiple steps
 * because the inner match type stays abstract.
 */
type AliasesOf[Ss <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => EmptyTuple
  case SourceEntry[?, ?, ?, a, ?] *: t => a *: AliasesOf[t]
}

/** Views tuple: `ColumnsView[EffectiveCols]` per committed source. */
type EffectiveViewsOf[Ss <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => EmptyTuple
  case SourceEntry[?, ?, c, ?, ?] *: t => ColumnsView[c] *: EffectiveViewsOf[t]
}

/** The view the `.where` / `.select` / `.orderBy` / `.groupBy` / `.having` lambdas receive. */
type JoinedView[Ss <: Tuple] =
  NamedTuple.NamedTuple[AliasesOf[Ss], EffectiveViewsOf[Ss]]

/** Aliases tuple for the `.on`-time view — committed aliases + pending alias (appended). */
type OnAliases[Ss <: Tuple, AR <: String & Singleton] <: Tuple = Ss match {
  case EmptyTuple                   => AR *: EmptyTuple
  case SourceEntry[?, ?, ?, a, ?] *: t => a *: OnAliases[t, AR]
}

/** Views tuple for the `.on`-time view — committed effective views + pending original view (appended). */
type OnViews[Ss <: Tuple, CR0 <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => ColumnsView[CR0] *: EmptyTuple
  case SourceEntry[?, ?, c, ?, ?] *: t => ColumnsView[c] *: OnViews[t, CR0]
}

/**
 * The view the `.on` lambda receives: committed sources use their effective cols; the pending source uses its
 * *original* cols because Postgres evaluates `ON` before NULL-padding.
 */
type OnView[Ss <: Tuple, PendingCols <: Tuple, PendingAlias <: String & Singleton] =
  NamedTuple.NamedTuple[OnAliases[Ss, PendingAlias], OnViews[Ss, PendingCols]]

// ---- View builders (runtime) -------------------------------------------------------------------

private[sharp] def buildJoinedView[Ss <: Tuple](sources: Ss): JoinedView[Ss] = {
  val views = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].map { s =>
    ColumnsView.qualified(s.effectiveCols, s.alias)
  }
  Tuple.fromArray(views.toArray[Any]).asInstanceOf[JoinedView[Ss]]
}

private[sharp] def buildOnView[Ss <: Tuple, CR0 <: Tuple, AR <: String & Singleton](
  sources: Ss,
  pendingOriginalCols: CR0,
  pendingAlias: AR
): OnView[Ss, CR0, AR] = {
  val committedViews = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]].map { s =>
    ColumnsView.qualified(s.effectiveCols, s.alias)
  }
  val pendingView = ColumnsView.qualified(pendingOriginalCols, pendingAlias)
  Tuple.fromArray((committedViews :+ pendingView).toArray[Any]).asInstanceOf[OnView[Ss, CR0, AR]]
}

// ---- IncompleteJoin ----------------------------------------------------------------------------

/**
 * Transitional state after `.innerJoin(x)` / `.leftJoin(x)` / `.rightJoin(x)` / `.fullJoin(x)`, before
 * `.on(predicate)`. Only `.on` is exposed — calling `.compile` / `.select` here is a compile error because the method
 * simply isn't there.
 *
 *   - `Ss` is the shape of *already-committed* sources as seen by the `ON` predicate: pre-null-padding for this
 *     particular join. Postgres evaluates `ON` before NULL-padding kicks in, so even for RIGHT / FULL the ON-view keeps
 *     the earlier sources' effective cols as they are today.
 *   - `SsFinal` is the shape that flows into the `SelectBuilder` the `.on` transitions to. For INNER / LEFT that equals
 *     `Ss`; for RIGHT / FULL it is `NullabilifySources[Ss]` — the WHERE / SELECT views on the final builder see the
 *     earlier sources' cols as `Option[_]` because a right-side-dominated join can NULL-pad them.
 */
final class IncompleteJoin[
  Ss <: Tuple,
  RR <: Relation[CR0],
  CR0 <: Tuple,
  CR <: Tuple, // = CR0 for INNER / RIGHT / CROSS, NullableCols[CR0] for LEFT / FULL
  AR <: String & Singleton,
  SsFinal <: Tuple // = Ss for INNER / LEFT / CROSS, NullabilifySources[Ss] for RIGHT / FULL
] private[sharp] (
  sources: Ss,
  pendingRelation: RR,
  pendingAlias: AR,
  pendingOriginalCols: CR0,
  pendingEffectiveCols: CR,
  kind: JoinKind,
  isLateral: Boolean = false
) {

  /**
   * Finalise the join predicate. The view sees every committed source's effective cols plus the pending source's
   * *original* cols — `ON` is evaluated before `NULL`-padding happens. Transitions to [[SelectBuilder]] with the
   * (possibly nullabilified) committed sources plus the pending source appended.
   *
   * The predicate's typed `Args` (`A`) is captured as the new source's `OnArgs` type member — outer `.compile`
   * picks it up via [[SourceOnArgsProj]] and surfaces it in the outer [[QueryTemplate]]'s `Args` so any [[Param]]
   * in an ON predicate composes statically with WHERE / GROUP BY / HAVING args. Plain column-to-column
   * comparisons (`r.a.id ==== r.b.user_id`) keep `A = Void` and the slot collapses to `Void`.
   */
  def on[A](
    f: OnView[Ss, CR0, AR] => skunk.sharp.TypedExpr[Boolean, A]
  ): SelectBuilder[Tuple.Append[SsFinal, SourceEntry[RR, CR0, CR, AR, A]], EmptyTuple, skunk.Void, skunk.Void] = {
    val rawPred = f(buildOnView[Ss, CR0, AR](sources, pendingOriginalCols, pendingAlias))
    val pred: Where[A] = rawPred
    val entry = new SourceEntry[RR, CR0, CR, AR, A](
      pendingRelation,
      pendingAlias,
      pendingOriginalCols,
      pendingEffectiveCols,
      kind,
      Some(pred),
      isLateral
    )
    val finalCommitted: Tuple = kind match {
      case JoinKind.Right | JoinKind.Full => nullabilifySources(sources)
      case _                              => sources
    }
    val nextSources = (finalCommitted :* entry).asInstanceOf[Tuple.Append[SsFinal, SourceEntry[RR, CR0, CR, AR, A]]]
    new SelectBuilder[Tuple.Append[SsFinal, SourceEntry[RR, CR0, CR, AR, A]], EmptyTuple, skunk.Void, skunk.Void](nextSources)
  }

}

// ---- FROM helpers -------------------------------------------------------------------------------

/**
 * Delegate rendering to each source's own `fromFragmentWith` kernel using the alias we captured when the source was
 * added. For plain sources (tables, views, CTEs) this calls `fromFragmentWith` directly. For typed subquery sources
 * (LATERAL join tail sources whose `bodyFragmentOpt` is set), the body is baked inline — only valid when the inner
 * query's args are all Void-collapsed (i.e. no `Param` in the LATERAL subquery). Typed-args LATERAL tail sources are
 * not yet supported and will throw at compile time.
 */
private[sharp] def aliasedFromEntry(s: SourceEntry[?, ?, ?, ?, ?]): skunk.AppliedFragment =
  s.relation.bodyFragmentOpt match {
    case Some(bodyFrag) =>
      val innerAf: skunk.AppliedFragment =
        if (bodyFrag.encoder.types.isEmpty)
          bodyFrag.asInstanceOf[skunk.Fragment[skunk.Void]].apply(skunk.Void)
        else
          throw new UnsupportedOperationException(
            "skunk-sharp: LATERAL join tail source with typed (non-Void) body args is not yet supported in multi-source queries"
          )
      skunk.sharp.TypedExpr.raw("(") |+| innerAf |+| skunk.sharp.TypedExpr.raw(s""") AS "${s.alias}"""")
    case None =>
      s.relation.fromFragmentWith(s.alias)
  }

/**
 * Build the FROM body parts for a source, producing a `List[BodyPart]`:
 *
 *  - **SRF source** (`IsSrf`): emits `Left("func("), Right(argsFrag), Left(") AS \"alias\" (\"col\")")`. The
 *    `Right(argsFrag)` slot carries the typed function-call args — `Param`s in `generate_series(start, stop)`
 *    or `unnest(arr)` thread into the outer query's Args.
 *  - **Typed subquery** (`bodyFragmentOpt = Some(frag)`): emits `Left("("), Right(innerFrag), Left(") AS alias")`.
 *    The `Right(innerFrag)` slot carries the typed inner `Fragment[BodyArgs]` — the assembler threads it as a
 *    positional slot in the outer query's Args chain.
 *  - **Plain source** (table / view / CTE, `bodyFragmentOpt = None`): emits `Left(fromFragmentWith(alias)),
 *    Right(emptyVoidSlot)`. The `Right(emptyVoidSlot)` is a positional placeholder (no SQL, no encode) so the
 *    source-body slot index stays stable regardless of whether the source is plain or typed.
 */
private[sharp] def aliasedFromEntryParts(s: SourceEntry[?, ?, ?, ?, ?]): List[SelectBuilder.BodyPart] =
  s.relation match {
    case srf: IsSrf =>
      List(
        Left(TypedExpr.raw(s"${srf.srfFuncName}(")),
        Right(srf.srfArgsFragment.asInstanceOf[Fragment[Any]]),
        Left(TypedExpr.raw(s""") AS "${s.alias}"("${srf.srfColumnName}")"""))
      )
    case _ =>
      s.relation.bodyFragmentOpt match {
        case Some(bodyFrag) =>
          List(
            Left(TypedExpr.raw("(")),
            Right(bodyFrag),
            Left(TypedExpr.raw(s""") AS "${s.alias}""""))
          )
        case None =>
          List(
            Left(s.relation.fromFragmentWith(s.alias)),
            Right(SelectBuilder.emptyVoidSlot)
          )
      }
  }

/**
 * Marker trait for set-returning-function relations (`generate_series`, `unnest`, …). Carries the function
 * name, typed args fragment, and output column name so [[aliasedFromEntryParts]] can render the strict SRF
 * shape `func(args) AS "alias" ("col")` (no outer parens — Postgres rejects `(func(args)) AS …`).
 */
private[sharp] trait IsSrf {
  def srfFuncName: String
  def srfArgsFragment: Fragment[?]
  def srfColumnName: String
}

// ---- SourceBodyArgs match type + SourceBodyArgsOf typeclass -----------------------------------

/**
 * Extract the `BodyArgs` type from a relation. Matches on [[TypedBodyRelation]] (which carries `BA` as
 * a direct class type parameter — not a type-member projection) before falling back to `skunk.Void` for
 * all plain relations (`Table`, `View`, CTE, re-aliased wrappers) which don't extend `TypedBodyRelation`.
 *
 * Using a helper match type (rather than `R#BodyArgs`) avoids the Scala 3 limitation that disallows
 * type-member projections (`#`) on pattern-bound type variables in match type bodies.
 */
private[dsl] type GetBodyArgs[R] = R match {
  case TypedBodyRelation[_, ba] => ba
  case _                        => skunk.Void
}

/**
 * Combined `BodyArgs` accumulator across all sources — a [[Where.Concat]] right-fold over per-source
 * `GetBodyArgs[r]`. Plain relations contribute `Void` (collapsed away); typed subquery relations contribute
 * their inner combined args. Used as the SELECT compile path's `SArgs` slot.
 */
type SourceBodyArgs[Ss <: Tuple] = Ss match {
  case EmptyTuple                      => skunk.Void
  case SourceEntry[r, ?, ?, ?, ?] *: tail => Where.Concat[GetBodyArgs[r], SourceBodyArgs[tail]]
}

/**
 * Project a combined [[SourceBodyArgs]] value back into a per-source list of body-args values. Mirrors the
 * fold structure of `SourceBodyArgs[Ss]`: at each `cons` step, peel one source's `BodyArgs` off the front via
 * [[Where.Concat2]] and recurse on the tail. The resulting `List[Any]` has one entry per source — `Void` for
 * plain sources, the typed inner-args value for typed-subquery sources — feeding the `IArray[Any]` that
 * `assembleN` consumes positionally.
 */
sealed trait SourceBodyArgsProj[Ss <: Tuple] {
  def project(combined: Any): List[Any]
}

object SourceBodyArgsProj {

  given empty: SourceBodyArgsProj[EmptyTuple] = new SourceBodyArgsProj[EmptyTuple] {
    def project(combined: Any): List[Any] = Nil
  }

  given cons[R <: Relation[C0], C0 <: Tuple, C <: Tuple, A <: String & Singleton, OA, T <: Tuple](using
    c2:   Where.Concat2[GetBodyArgs[R], SourceBodyArgs[T]],
    rest: SourceBodyArgsProj[T]
  ): SourceBodyArgsProj[SourceEntry[R, C0, C, A, OA] *: T] = new SourceBodyArgsProj[SourceEntry[R, C0, C, A, OA] *: T] {
    def project(combined: Any): List[Any] = {
      val (h, t) = c2.project(combined.asInstanceOf[Where.Concat[GetBodyArgs[R], SourceBodyArgs[T]]])
      h :: rest.project(t)
    }
  }

}

/**
 * Combined ON-predicate args accumulator — a [[Where.Concat]] right-fold over per-source `OnArgs`. Sources whose
 * ON predicate is absent (head source, CROSS joins) carry `Void` and collapse out.
 */
type SourceOnArgs[Ss <: Tuple] = Ss match {
  case EmptyTuple                          => skunk.Void
  case SourceEntry[?, ?, ?, ?, oa] *: tail => Where.Concat[oa, SourceOnArgs[tail]]
}

/**
 * Typeclass wrapper around [[SourceOnArgs]] — gives the standard `Aux[Ss, O]` shape so the compile path can
 * summon it as a using parameter and read out the combined `OnA` type without writing the match type inline.
 */
trait SourceOnArgsOf[Ss <: Tuple] {
  type Out
}

object SourceOnArgsOf {
  type Aux[Ss <: Tuple, O] = SourceOnArgsOf[Ss] { type Out = O }

  given compute[Ss <: Tuple]: (SourceOnArgsOf[Ss] { type Out = SourceOnArgs[Ss] }) =
    new SourceOnArgsOf[Ss] { type Out = SourceOnArgs[Ss] }
}

/**
 * Project a combined [[SourceOnArgs]] value back into a per-source list of ON-pred-args values, in source order.
 * Used by the SELECT compile path to fill the per-source ON slot in the IArray that `assembleN` consumes.
 */
sealed trait SourceOnArgsProj[Ss <: Tuple] {
  def project(combined: Any): List[Any]
}

object SourceOnArgsProj {

  given empty: SourceOnArgsProj[EmptyTuple] = new SourceOnArgsProj[EmptyTuple] {
    def project(combined: Any): List[Any] = Nil
  }

  given cons[R <: Relation[C0], C0 <: Tuple, C <: Tuple, A <: String & Singleton, OA, T <: Tuple](using
    c2:   Where.Concat2[OA, SourceOnArgs[T]],
    rest: SourceOnArgsProj[T]
  ): SourceOnArgsProj[SourceEntry[R, C0, C, A, OA] *: T] = new SourceOnArgsProj[SourceEntry[R, C0, C, A, OA] *: T] {
    def project(combined: Any): List[Any] = {
      val (h, t) = c2.project(combined.asInstanceOf[Where.Concat[OA, SourceOnArgs[T]]])
      h :: rest.project(t)
    }
  }

}

/**
 * Typeclass wrapper around [[SourceBodyArgs]] — gives the standard `Aux[Ss, O]` pattern expected by
 * [[SelectBuilder.compile]], [[SelectBuilder.compileBodyFragment]], and the `.alias` extensions.
 * A single `given compute` always provides `Out = SourceBodyArgs[Ss]`, which the Scala 3 compiler
 * reduces to a concrete type when `Ss` is known.
 */
trait SourceBodyArgsOf[Ss <: Tuple] {
  type Out
}

object SourceBodyArgsOf {

  type Aux[Ss <: Tuple, O] = SourceBodyArgsOf[Ss] { type Out = O }

  given compute[Ss <: Tuple]: (SourceBodyArgsOf[Ss] { type Out = SourceBodyArgs[Ss] }) =
    new SourceBodyArgsOf[Ss] { type Out = SourceBodyArgs[Ss] }

}

// ---- Join entry points -------------------------------------------------------------------------

/**
 * `.innerJoin` / `.leftJoin` / `.crossJoin` on any relation-like value — bare `Table` / `View` (auto-aliased to its own
 * name) or an already-aliased `Relation` from `.alias("…")` / `<sb>.alias("…")` / `<setOp>.alias("…")`.
 */
extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton, ML <: AliasMode](left: L)(using
  aL: AsRelation.Aux[L, RL, CL, AL, ML]
) {

  /**
   * `INNER JOIN` — neither side's columns nullabilified. Requires the right side's alias to be distinct from the
   * left's; a self-join with two implicit aliases like `users.innerJoin(users)` is rejected at compile time — use
   * `.alias("u1")` / `.alias("u2")` on either side to disambiguate.
   */
  def innerJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    CR,
    AR,
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple
  ] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val rel       = aR(right)
    val rCols     = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(baseEntry *: EmptyTuple, rel, aR.aliasValue(right), rCols, rCols, JoinKind.Inner)
  }

  /** `LEFT JOIN` — right-side column value types wrap in `Option`; `.opt` on the codecs at runtime. */
  def leftJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    NullableCols[CR],
    AR,
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple
  ] = {
    val baseEntry    = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val rel          = aR(right)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(baseEntry *: EmptyTuple, rel, aR.aliasValue(right), origCols, effectiveCls, JoinKind.Left)
  }

  /**
   * `RIGHT JOIN` — left-side column value types wrap in `Option`. Mirror of LEFT: the already-committed source may be
   * NULL-padded when the right side dominates; the right side's columns keep their declared types. Chains further: a
   * subsequent JOIN sees every earlier source's cols as `Option[_]`.
   */
  def rightJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    CR,
    AR,
    NullabilifySources[SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple]
  ] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val rel       = aR(right)
    val rCols     = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(baseEntry *: EmptyTuple, rel, aR.aliasValue(right), rCols, rCols, JoinKind.Right)
  }

  /** `FULL OUTER JOIN` — both sides wrap in `Option`. Union of LEFT and RIGHT in type-level effect. */
  def fullJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    NullableCols[CR],
    AR,
    NullabilifySources[SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple]
  ] = {
    val baseEntry    = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val rel          = aR(right)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(baseEntry *: EmptyTuple, rel, aR.aliasValue(right), origCols, effectiveCls, JoinKind.Full)
  }

  /** `CROSS JOIN` — no predicate required; transitions straight to a two-source [[SelectBuilder]]. */
  def crossJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): SelectBuilder[(SourceEntry[RL, CL, CL, AL, Void], SourceEntry[RR, CR, CR, AR, Void]), EmptyTuple, skunk.Void, skunk.Void] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val rel       = aR(right)
    val rCols     = rel.columns.asInstanceOf[CR]
    val rEntry    =
      new SourceEntry[RR, CR, CR, AR, Void](rel, aR.aliasValue(right), rCols, rCols, JoinKind.Cross, None)
    new SelectBuilder[(SourceEntry[RL, CL, CL, AL, Void], SourceEntry[RR, CR, CR, AR, Void]), EmptyTuple, skunk.Void, skunk.Void]((baseEntry, rEntry))
  }

  // ---- LATERAL joins ---------------------------------------------------------------------------
  //
  // `LATERAL` unlocks left-to-right correlation in FROM: the subquery on the right of a LATERAL join can reference
  // the outer source's columns. The user's lambda receives the outer's qualified `ColumnsView`, which means
  // correlated predicates in the inner `.where` — `p.user_id ==== outer.id` — type-check and render as
  // alias-qualified SQL that Postgres resolves against the outer FROM-clause entry.

  /**
   * `INNER JOIN LATERAL (subquery) AS "x" ON <predicate>` — correlated inner join. The lambda receives the outer
   * source's columns so the inner query's `.where` / `.limit` can reference them.
   */
  def innerJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: ColumnsView[CL] => T
  )(using
    aR: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    CR,
    AR,
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple
  ] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val outer     = ColumnsView.qualified(baseEntry.effectiveCols, baseEntry.alias)
    val t         = fn(outer)
    val rel       = aR(t)
    val rCols     = rel.columns.asInstanceOf[CR]
    new IncompleteJoin(baseEntry *: EmptyTuple, rel, aR.aliasValue(t), rCols, rCols, JoinKind.Inner, isLateral = true)
  }

  /**
   * `LEFT JOIN LATERAL (subquery) AS "x" ON <predicate>` — correlated left join. When the inner subquery produces no
   * rows for an outer row, the outer row still surfaces with the lateral side NULL-padded (so inner cols decode as
   * `Option[_]`).
   */
  def leftJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: ColumnsView[CL] => T
  )(using
    aR: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): IncompleteJoin[
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple,
    RR,
    CR,
    NullableCols[CR],
    AR,
    SourceEntry[RL, CL, CL, AL, Void] *: EmptyTuple
  ] = {
    val baseEntry    = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val outer        = ColumnsView.qualified(baseEntry.effectiveCols, baseEntry.alias)
    val t            = fn(outer)
    val rel          = aR(t)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin(
      baseEntry *: EmptyTuple,
      rel,
      aR.aliasValue(t),
      origCols,
      effectiveCls,
      JoinKind.Left,
      isLateral = true
    )
  }

  /**
   * `CROSS JOIN LATERAL (subquery) AS "x"` — correlated cross join, no ON required. Typical shape for "expand each
   * outer row by the rows of a per-row-parameterised subquery". Outer rows whose inner produces zero rows are dropped
   * (same semantics as a regular CROSS JOIN).
   */
  def crossJoinLateral[T, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton, MR <: AliasMode](
    fn: ColumnsView[CL] => T
  )(using
    aR: AsRelation.Aux[T, RR, CR, AR, MR],
    aliasCheck: AliasNotUsed[AR, AL *: EmptyTuple]
  ): SelectBuilder[(SourceEntry[RL, CL, CL, AL, Void], SourceEntry[RR, CR, CR, AR, Void]), EmptyTuple, skunk.Void, skunk.Void] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL, ML](aL, left)
    val outer     = ColumnsView.qualified(baseEntry.effectiveCols, baseEntry.alias)
    val t         = fn(outer)
    val rel       = aR(t)
    val rCols     = rel.columns.asInstanceOf[CR]
    val rEntry    =
      new SourceEntry[RR, CR, CR, AR, Void](
        rel,
        aR.aliasValue(t),
        rCols,
        rCols,
        JoinKind.Cross,
        None,
        isLateral = true
      )
    new SelectBuilder[(SourceEntry[RL, CL, CL, AL, Void], SourceEntry[RR, CR, CR, AR, Void]), EmptyTuple, skunk.Void, skunk.Void]((baseEntry, rEntry))
  }

}

private[sharp] def makeBaseEntry[L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton, ML <: AliasMode](
  aL: AsRelation.Aux[L, RL, CL, AL, ML],
  left: L
): SourceEntry[RL, CL, CL, AL, Void] = {
  val rel  = aL(left)
  val cols = rel.columns.asInstanceOf[CL]
  new SourceEntry[RL, CL, CL, AL, Void](rel, aL.aliasValue(left), cols, cols, JoinKind.Inner, None)
}
