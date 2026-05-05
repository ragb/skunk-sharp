package skunk.sharp.dsl

import skunk.{AppliedFragment, Fragment, Void}
import skunk.sharp.*
import skunk.sharp.where.Where

/**
 * Marker for any relation whose FROM rendering references a CTE name rather than a base table. Both [[CteRelation]]
 * itself (whether under its default alias or re-aliased via `.alias("x")`) implements this trait so
 * [[collectCtesInOrder]] can detect it regardless of how many alias layers are on top.
 */
private[dsl] trait IsCte {
  def underlyingCte: CteRelation[?, ?, ?, ?]
}

/**
 * A named CTE — the value returned by [[cte]]. Extends [[Relation]] so it slots directly into `.select`, `.innerJoin`,
 * `.leftJoin`, and every other FROM-position verb.
 *
 * In a FROM clause it renders as just its name (`"name"` or `"name" AS "alias"`). The `WITH "name" AS (body)` preamble
 * is emitted once, at the outermost `.compile` call, where the body's typed `BodyArgs` are bound as a single Right slot
 * in the assembled IArray. Multiple references to the same CTE from FROM positions still produce a single WITH entry
 * (and a single args binding).
 *
 * `BodyArgs` is the captured-args type of the inner SELECT. `Param`s in the inner WHERE / HAVING / GROUP BY surface
 * here as a typed tuple, threaded into the outer query's `Args` via [[CteArgs]].
 *
 * `Alias_` is decoupled from `Name`: a CTE created via `cte("active", …)` has `Alias_ = Name = "active"`. Calling
 * `.alias("x")` returns a re-aliased `CteRelation[Cols, "active", "x", BodyArgs]` so its `BodyArgs` stays visible at
 * the type level (essential for typed-args CTEs to thread their inner Params into the outer compile after re-alias).
 *
 * `deps` records which other CTEs this one directly references (captured at creation time). [[collectCtesInOrder]] does
 * a depth-first walk over deps so chained CTEs are always emitted in the right dependency order. Typed-args CTEs may
 * not appear as deps — they must be referenced directly in the outer query's FROM (enforced at [[cte]] time via
 * [[CteDepsAllVoid]]).
 */
final class CteRelation[Cols <: Tuple, Name <: String & Singleton, Alias_ <: String & Singleton, BodyArgsT] private[sharp] (
  val cteName: Name,
  val aliasName: Alias_,
  private[sharp] val body: () => Fragment[BodyArgsT],
  private[sharp] val deps: List[CteRelation[?, ?, ?, ?]],
  private[sharp] val cols0: Cols
) extends Relation[Cols] with IsCte {
  type Alias    = Alias_
  type Mode     = AliasMode.Explicit
  type BodyArgs = skunk.Void  // FROM-site contribution; the typed body args bind at the WITH preamble.

  /** Typed inner-body args — surfaced in outer compile via [[CteArgs]] / [[CteArgsProj]]. */
  type CteBody = BodyArgsT

  def currentAlias: Alias_      = aliasName
  def name: String              = cteName
  def schema: Option[String]    = None
  def columns: Cols             = cols0
  def expectedTableType: String = ""
  def underlyingCte: this.type  = this

  override def fromFragmentWith(a: String): AppliedFragment =
    if (a == cteName) TypedExpr.raw(s""""$cteName"""")
    else TypedExpr.raw(s""""$cteName" AS "$a"""")

}

/**
 * Lift a whole-row SELECT into a named CTE.
 *
 * {{{
 *   val active = cte("active", users.select.where(u => u.deleted_at.isNull))
 *   active.select.compile   // WITH "active" AS (SELECT … FROM "users" WHERE …) SELECT … FROM "active"
 * }}}
 *
 * The returned [[CteRelation]] can be joined, re-aliased, and used anywhere a [[Relation]] is accepted. Multiple CTEs
 * in the same query are collected and deduplicated at compile time — each `WITH` entry appears only once, in dependency
 * order.
 */
def cte[Ss <: Tuple, GroupsT <: Tuple, WA, HA, N <: String & Singleton, SArgs, GArgs](
  name: N,
  query: SelectBuilder[Ss, GroupsT, WA, HA]
)(using
  ev:    IsSingleSource[Ss],
  sbOf:  SourceBodyArgsOf.Aux[Ss, SArgs],
  g:     ProjArgsOf.Aux[GroupsT, GArgs],
  cs:    Where.Concat2[SArgs, WA],
  csg:   Where.Concat2[Where.Concat[SArgs, WA], GArgs],
  csgh:  Where.Concat2[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA],
  noTypedDeps: CteDepsAllVoid[Ss]
): CteRelation[ev.Cols, N, N, Where.Concat[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]] = {
  type Combined = Where.Concat[Where.Concat[Where.Concat[SArgs, WA], GArgs], HA]
  val entries = query.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
  val deps    = directCtes(entries)
  val cols    = entries.head.effectiveCols.asInstanceOf[ev.Cols]
  // Captures Ss-evidences in the closure so the body re-renders if `body()` is called more than once.
  val bodyThunk: () => Fragment[Combined] = () =>
    query.compileBodyFragment[SArgs, GArgs](using ev, sbOf, g, cs, csg, csgh)
  new CteRelation[ev.Cols, N, N, Combined](name, name, bodyThunk, deps, cols)
}

/**
 * Lift a projected SELECT into a named CTE.
 *
 * Every column in the projection must carry a name — either a bare [[TypedColumn]] or an [[AliasedExpr]] — so the CTE
 * columns are well-defined. This is the same constraint as `ProjectedSelect.alias`.
 *
 * {{{
 *   val totals = cte("totals",
 *     orders.select(o => (o.user_id, Pg.sum(o.amount).as("total"))).groupBy(o => o.user_id)
 *   )
 *   totals.innerJoin(users).on(r => r.totals.user_id ==== r.users.id).select(r => (r.users.email, r.totals.total)).compile
 * }}}
 */
def cte[Ss <: Tuple, Proj <: Tuple, Groups <: Tuple, DA <: Tuple, OA <: Tuple, WA, HA, Row, N <: String & Singleton,
        SA, OnA, DA2, PA, GA, OA2](
  name: N,
  query: ProjectedSelect[Ss, Proj, Groups, DA, OA, WA, HA, Row]
)(using
  gc:       GroupCoverage[Proj, Groups],
  @scala.annotation.unused np: AllNamedProj[Proj],
  sbOf:     SourceBodyArgsOf.Aux[Ss, SA],
  bff:      SourceBodyArgsProj[Ss],
  onSum:    SourceOnArgsOf.Aux[Ss, OnA],
  onProj:   SourceOnArgsProj[Ss],
  d:        ProjArgsOf.Aux[DA, DA2],
  pa:       ProjArgsOf.Aux[Proj, PA],
  gp:       ProjArgsOf.Aux[Groups, GA],
  o:        ProjArgsOf.Aux[OA, OA2],
  dp:       Where.Concat2[DA2, PA],
  dps:      Where.Concat2[Where.Concat[DA2, PA], SA],
  dpso:     Where.Concat2[Where.Concat[Where.Concat[DA2, PA], SA], OnA],
  dpsow:    Where.Concat2[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA],
  dpsowg:   Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA],
  dpsowgh:  Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA],
  dpsowgho: Where.Concat2[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2],
  noTypedDeps: CteDepsAllVoid[Ss]
): CteRelation[ProjCols[Proj], N, N, Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]] = {
  type Combined = Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[Where.Concat[DA2, PA], SA], OnA], WA], GA], HA], OA2]
  val entries = query.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?, ?]]]
  val deps    = directCtes(entries)
  val cols    = buildProjectedCols(query.projections).asInstanceOf[ProjCols[Proj]]
  val bodyThunk: () => Fragment[Combined] = () =>
    query.compileBodyFragment[SA, OnA, DA2, PA, GA, OA2](
      using gc, sbOf, bff, onSum, onProj, d, pa, gp, o, dp, dps, dpso, dpsow, dpsowg, dpsowgh, dpsowgho
    )
  new CteRelation[ProjCols[Proj], N, N, Combined](name, name, bodyThunk, deps, cols)
}

// ---- CTE collection helpers (used by SelectBuilder.compile and ProjectedSelect.compile) --------

/** Extract the directly-referenced CTEs from a source-entry list (handles re-aliased CteRelations via [[IsCte]]). */
private[dsl] def directCtes(entries: List[SourceEntry[?, ?, ?, ?, ?]]): List[CteRelation[?, ?, ?, ?]] =
  entries.collect {
    case e if e.relation.isInstanceOf[IsCte] =>
      e.relation.asInstanceOf[IsCte].underlyingCte
  }

/**
 * Walk `entries`, find all CTE relations (transitively through their `deps`), and return them in dependency order
 * (earliest dependency first). Duplicate names are visited only once.
 */
private[dsl] def collectCtesInOrder(entries: List[SourceEntry[?, ?, ?, ?, ?]]): List[CteRelation[?, ?, ?, ?]] = {
  val result                               = scala.collection.mutable.ListBuffer.empty[CteRelation[?, ?, ?, ?]]
  val visited                              = scala.collection.mutable.LinkedHashSet.empty[String]
  def visit(c: CteRelation[?, ?, ?, ?]): Unit =
    if (!visited.contains(c.cteName)) {
      visited += c.cteName
      c.deps.foreach(visit)
      result += c
    }
  directCtes(entries).foreach(visit)
  result.toList
}

/**
 * Emit the `WITH …` preamble as a list of [[SelectBuilder.BodyPart]]s. Each CTE's typed body fragment is a single
 * `Right` slot; structural keywords (`WITH `, `AS (`, `)`, `, `, trailing space) are `Left` AppliedFragments. Returns
 * an empty list when `ctes` is empty so callers can prepend without any `WITH ` chunk.
 *
 * The slot order matches the dep-walk order: dep CTEs before their dependents. The outer compile's `slotValues`
 * IArray must place per-CTE body args in this same order, ahead of all body slots.
 */
private[dsl] def renderWithPreambleParts(ctes: List[CteRelation[?, ?, ?, ?]]): List[SelectBuilder.BodyPart] = {
  if (ctes.isEmpty) Nil
  else {
    val buf = scala.collection.mutable.ListBuffer.empty[SelectBuilder.BodyPart]
    buf += Left(TypedExpr.raw("WITH "))
    var first = true
    ctes.foreach { c =>
      if (first) first = false else buf += Left(TypedExpr.raw(", "))
      buf += Left(TypedExpr.raw(s""""${c.cteName}" AS ("""))
      buf += Right(c.body().asInstanceOf[Fragment[Any]])
      buf += Left(TypedExpr.raw(")"))
    }
    buf += Left(TypedExpr.raw(" "))
    buf.toList
  }
}

// ---- CTE typed-args plumbing -------------------------------------------------------------------

/**
 * Type-level extraction of a relation's CTE body args. Matches `CteRelation[_, _, ba]` directly (CteRelation is final,
 * so reduction terminates), falling back to `Void` for any other relation. Used by [[CteArgs]] to fold over a sources
 * tuple.
 */
private[dsl] type GetCteBody[R] = R match {
  case CteRelation[_, _, _, ba] => ba
  case _                     => skunk.Void
}

/**
 * Combined CTE-body-args accumulator over a sources tuple — sums (via [[Where.Concat]]) the body args of every direct
 * CteRelation reference in the tuple. Indirect (transitive) CTE deps must have `BodyArgs = Void` (enforced by
 * [[CteDepsAllVoid]] at `cte()` construction), so they don't appear here.
 */
type CteArgs[Ss <: Tuple] = Ss match {
  case EmptyTuple                          => skunk.Void
  case SourceEntry[r, ?, ?, ?, ?] *: tail => Where.Concat[GetCteBody[r], CteArgs[tail]]
}

/**
 * Typeclass wrapper around [[CteArgs]] — provides the standard `Aux[Ss, O]` shape so the compile path can summon it
 * as a using parameter and bind the combined `CArgs` type without writing the match type inline.
 */
trait CteArgsOf[Ss <: Tuple] {
  type Out
}

object CteArgsOf {
  type Aux[Ss <: Tuple, O] = CteArgsOf[Ss] { type Out = O }

  given compute[Ss <: Tuple]: (CteArgsOf[Ss] { type Out = CteArgs[Ss] }) =
    new CteArgsOf[Ss] { type Out = CteArgs[Ss] }
}

/**
 * Project a combined [[CteArgs]] value back into a per-direct-CTE list of body-args values, in source order. Plain
 * (non-CteRelation) sources contribute `Void` (skipped at IArray fill time).
 */
sealed trait CteArgsProj[Ss <: Tuple] {
  def project(combined: Any): List[Any]
}

object CteArgsProj {

  given empty: CteArgsProj[EmptyTuple] = new CteArgsProj[EmptyTuple] {
    def project(combined: Any): List[Any] = Nil
  }

  given cons[R <: Relation[C0], C0 <: Tuple, C <: Tuple, A <: String & Singleton, OA, T <: Tuple](using
    c2:   Where.Concat2[GetCteBody[R], CteArgs[T]],
    rest: CteArgsProj[T]
  ): CteArgsProj[SourceEntry[R, C0, C, A, OA] *: T] = new CteArgsProj[SourceEntry[R, C0, C, A, OA] *: T] {
    def project(combined: Any): List[Any] = {
      val (h, t) = c2.project(combined.asInstanceOf[Where.Concat[GetCteBody[R], CteArgs[T]]])
      h :: rest.project(t)
    }
  }

}

/**
 * Compile-time guard summoned by [[cte]]: every directly-referenced CTE in the body's source tuple must have
 * `BodyArgs = Void`. Typed-args CTEs cannot be transitive deps — they must surface as direct references in the outer
 * query so their args slot is type-level visible at the outer compile site.
 */
sealed trait CteDepsAllVoid[Ss <: Tuple]

object CteDepsAllVoid {

  given empty: CteDepsAllVoid[EmptyTuple] = new CteDepsAllVoid[EmptyTuple] {}

  given cons[R <: Relation[C0], C0 <: Tuple, C <: Tuple, A <: String & Singleton, OA, T <: Tuple](using
    @scala.annotation.unused ev: GetCteBody[R] =:= skunk.Void,
    rest: CteDepsAllVoid[T]
  ): CteDepsAllVoid[SourceEntry[R, C0, C, A, OA] *: T] = new CteDepsAllVoid[SourceEntry[R, C0, C, A, OA] *: T] {}

}

/**
 * Per-CTE body args: maps a list of `CteRelation[?, ?, ?, ?]` (in dep order) to a `List[Any]` of body-args values to
 * inject at the WITH preamble's Right slots. Each entry is the value paired with that CTE's Right-slot Fragment.
 * Plain (Void) bodies pass `Void`; typed bodies pass the captured args from the outer query's combined `CteArgs`.
 *
 * The outer compile builds `cteSlotValues` by walking `collectCtesInOrder(entries)` and matching each CTE against the
 * direct-ref list (whose body args are projected via [[CteArgsProj]]). CTEs appearing only as transitive deps map to
 * `Void` (their bodies are constrained Void by [[CteDepsAllVoid]]).
 */
private[dsl] def buildCteSlotValues(
  collectedCtes: List[CteRelation[?, ?, ?, ?]],
  directRefArgs: Map[String, Any]
): List[Any] =
  collectedCtes.map(c => directRefArgs.getOrElse(c.cteName, Void))
