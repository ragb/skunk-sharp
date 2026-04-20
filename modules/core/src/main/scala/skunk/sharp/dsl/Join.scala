package skunk.sharp.dsl

import skunk.{AppliedFragment, Codec}
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

  def alias[A <: String & Singleton](a: A): Relation[Cols] { type Alias = A } = {
    val underlying = r
    val newAlias   = a
    new Relation[Cols] {
      type Alias = A
      val currentAlias: A                 = newAlias
      def name: String                    = underlying.name
      def columns: Cols                   = underlying.columns
      def schema: Option[String]          = underlying.schema
      def expectedTableType: String       = underlying.expectedTableType
      override def hasFromClause: Boolean = underlying.hasFromClause
      override def qualifiedName: String  = underlying.qualifiedName
      // Delegate the rendering kernel — the underlying decides table-vs-view-vs-subquery shape; we only swap the
      // alias it's asked to use.
      override def fromFragmentWith(x: String): AppliedFragment = underlying.fromFragmentWith(x)
    }
  }

}

/**
 * `.alias("name")` on a single-source whole-row [[SelectBuilder]] — promote the query to a joinable [[Relation]] in a
 * FROM position.
 *
 * Postgres requires every derived table to carry an alias; this extension makes the alias mandatory by construction — a
 * bare `SelectBuilder` does not satisfy `AsRelation`, so the compiler rejects it in JOIN / `.select` position until the
 * user calls `.alias("x")`. The returned `Relation[Cols] { type Alias = A }` is the same shape Table and View produce;
 * after this point, the subquery is indistinguishable from any other relation to the join machinery.
 *
 * The inner SQL is held **lazily** as a thunk — no `AppliedFragment` is rendered until the outer query's `.compile`
 * walks this source. One terminal `.compile` for the whole tree.
 *
 * {{{
 *   val active = users.select.where(u => u.deleted_at.isNull).alias("active")
 *   active.innerJoin(orders).on(r => r.active.id ==== r.orders.user_id).compile
 * }}}
 *
 * Available on single-source whole-row builders today via `IsSingleSource` evidence. Projection-typed subqueries and
 * set-op results need a little more `Cols`-metadata threading; `.asExpr` (scalar-subquery-as-expression) covers the
 * common cases in the meantime.
 */
extension [Ss <: Tuple](sb: SelectBuilder[Ss])(using ev: IsSingleSource[Ss]) {

  def alias[A <: String & Singleton](a: A): Relation[ev.Cols] { type Alias = A } = {
    val newAlias = a
    val cols = sb.sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].head.effectiveCols.asInstanceOf[ev.Cols]
    // Inner compile runs only when `fromFragmentWith` is invoked (during outer-query rendering).
    val renderInner: () => AppliedFragment = () => sb.compile(using ev).af
    new Relation[ev.Cols] {
      type Alias = A
      val currentAlias: A           = newAlias
      val name: String              = newAlias // derived relations have no separate identity — alias IS the name
      val schema: Option[String]    = None
      val columns: ev.Cols          = cols
      val expectedTableType: String = ""       // marker: not validated against `information_schema.tables`
      override def fromFragmentWith(x: String): AppliedFragment =
        TypedExpr.raw("(") |+| renderInner() |+| TypedExpr.raw(s""") AS "$x"""")
    }
  }

}

// ---- JoinKind + NullableCols -------------------------------------------------------------------

/** Kind of join — drives the rendered keyword and whether the right-side cols are nullabilified. */
enum JoinKind(val sql: String) {
  case Inner extends JoinKind("INNER JOIN")
  case Left  extends JoinKind("LEFT JOIN")
  case Cross extends JoinKind("CROSS JOIN")
}

/**
 * Type-level "make every column nullable" — wraps each value type in `Option` and flips the Null phantom to `true`.
 * Used for sources attached via LEFT JOIN, where all their columns may be `NULL` when no match is found.
 */
type NullableCols[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, attrs] *: tail => Column[Option[t], n, true, attrs] *: NullableCols[tail]
  case EmptyTuple                      => EmptyTuple
}

/**
 * Runtime counterpart to [[NullableCols]]: walk the columns tuple and wrap each codec in `.opt` so the decoder emits
 * `Option[T]`. Name / tpe / flags are preserved.
 */
private[sharp] def nullabilifyCols(cols: Tuple): Tuple = {
  val wrapped = cols.toList.map {
    case c: Column[?, ?, ?, ?] =>
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
  def apply(t: T): Rel

  /**
   * Retrieve the alias as an `Alias`-typed value. Exists because path-dependent access `rel.alias` from a caller scope
   * where `Rel` is an abstract type parameter doesn't auto-reduce to `Alias` — Scala won't collapse the refinement
   * `Rel { type Alias = Alias }` at use sites. The typeclass witnesses the equality once, here, and hands the value
   * back at the refined type so call sites don't need `.asInstanceOf`.
   */
  def aliasValue(t: T): Alias
}

object AsRelation {

  type Aux[T, R0 <: Relation[C0], C0 <: Tuple, A <: String & Singleton] = AsRelation[T] {
    type Rel   = R0
    type Cols  = C0
    type Alias = A
  }

  /**
   * The only given — any `Relation[CC]` with a known `Alias = A` satisfies the contract. Table / View / the subquery
   * relation produced by `sb.alias("x")` / the re-aliased wrapper all flow through this single instance.
   */
  given fromRelation[R <: Relation[CC] { type Alias = A }, CC <: Tuple, A <: String & Singleton]
    : AsRelation.Aux[R, R, CC, A] = new AsRelation[R] {
    type Rel   = R
    type Cols  = CC
    type Alias = A
    def apply(r: R): R      = r
    def aliasValue(r: R): A = r.currentAlias
  }

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
  Alias <: String & Singleton
] private[sharp] (
  val relation: R,
  val alias: Alias,
  val originalCols: Cols0,
  val effectiveCols: Cols,
  val kind: JoinKind,          // for the base source this is `Inner` (cosmetic — not rendered for index 0)
  val onPredOpt: Option[Where] // `None` for the base source and CROSS joins; `Some` for INNER/LEFT
)

/**
 * Aliases tuple: one alias literal per committed source. Pattern-matches `SourceEntry[?, ?, ?, a]` directly in the `*:`
 * arm — going through a separate `AliasOf` helper breaks reduction when `Ss` is constructed through multiple steps
 * because the inner match type stays abstract.
 */
type AliasesOf[Ss <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => EmptyTuple
  case SourceEntry[?, ?, ?, a] *: t => a *: AliasesOf[t]
}

/** Views tuple: `ColumnsView[EffectiveCols]` per committed source. */
type EffectiveViewsOf[Ss <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => EmptyTuple
  case SourceEntry[?, ?, c, ?] *: t => ColumnsView[c] *: EffectiveViewsOf[t]
}

/** The view the `.where` / `.select` / `.orderBy` / `.groupBy` / `.having` lambdas receive. */
type JoinedView[Ss <: Tuple] =
  NamedTuple.NamedTuple[AliasesOf[Ss], EffectiveViewsOf[Ss]]

/** Aliases tuple for the `.on`-time view — committed aliases + pending alias (appended). */
type OnAliases[Ss <: Tuple, AR <: String & Singleton] <: Tuple = Ss match {
  case EmptyTuple                   => AR *: EmptyTuple
  case SourceEntry[?, ?, ?, a] *: t => a *: OnAliases[t, AR]
}

/** Views tuple for the `.on`-time view — committed effective views + pending original view (appended). */
type OnViews[Ss <: Tuple, CR0 <: Tuple] <: Tuple = Ss match {
  case EmptyTuple                   => ColumnsView[CR0] *: EmptyTuple
  case SourceEntry[?, ?, c, ?] *: t => ColumnsView[c] *: OnViews[t, CR0]
}

/**
 * The view the `.on` lambda receives: committed sources use their effective cols; the pending source uses its
 * *original* cols because Postgres evaluates `ON` before NULL-padding.
 */
type OnView[Ss <: Tuple, PendingCols <: Tuple, PendingAlias <: String & Singleton] =
  NamedTuple.NamedTuple[OnAliases[Ss, PendingAlias], OnViews[Ss, PendingCols]]

// ---- View builders (runtime) -------------------------------------------------------------------

private[sharp] def buildJoinedView[Ss <: Tuple](sources: Ss): JoinedView[Ss] = {
  val views = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].map { s =>
    ColumnsView.qualified(s.effectiveCols, s.alias)
  }
  Tuple.fromArray(views.toArray[Any]).asInstanceOf[JoinedView[Ss]]
}

private[sharp] def buildOnView[Ss <: Tuple, CR0 <: Tuple, AR <: String & Singleton](
  sources: Ss,
  pendingOriginalCols: CR0,
  pendingAlias: AR
): OnView[Ss, CR0, AR] = {
  val committedViews = sources.toList.asInstanceOf[List[SourceEntry[?, ?, ?, ?]]].map { s =>
    ColumnsView.qualified(s.effectiveCols, s.alias)
  }
  val pendingView = ColumnsView.qualified(pendingOriginalCols, pendingAlias)
  Tuple.fromArray((committedViews :+ pendingView).toArray[Any]).asInstanceOf[OnView[Ss, CR0, AR]]
}

// ---- IncompleteJoin ----------------------------------------------------------------------------

/**
 * Transitional state after `.innerJoin(x)` or `.leftJoin(x)`, before `.on(predicate)`. Only `.on` is exposed — calling
 * `.compile` / `.select` here is a compile error because the method simply isn't there.
 */
final class IncompleteJoin[
  Ss <: Tuple,
  RR <: Relation[CR0],
  CR0 <: Tuple,
  CR <: Tuple, // = CR0 for INNER, NullableCols[CR0] for LEFT
  AR <: String & Singleton
] private[sharp] (
  sources: Ss,
  pendingRelation: RR,
  pendingAlias: AR,
  pendingOriginalCols: CR0,
  pendingEffectiveCols: CR,
  kind: JoinKind
) {

  /**
   * Finalise the join predicate. The view sees every committed source's effective cols plus the pending source's
   * *original* cols — `ON` is evaluated before `NULL`-padding happens. Transitions to [[SelectBuilder]] with the
   * pending source appended to `Ss`.
   */
  def on(
    f: OnView[Ss, CR0, AR] => Where
  ): SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR0, CR, AR]]] = {
    val pred  = f(buildOnView[Ss, CR0, AR](sources, pendingOriginalCols, pendingAlias))
    val entry = new SourceEntry[RR, CR0, CR, AR](
      pendingRelation,
      pendingAlias,
      pendingOriginalCols,
      pendingEffectiveCols,
      kind,
      Some(pred)
    )
    val nextSources = (sources :* entry).asInstanceOf[Tuple.Append[Ss, SourceEntry[RR, CR0, CR, AR]]]
    new SelectBuilder[Tuple.Append[Ss, SourceEntry[RR, CR0, CR, AR]]](nextSources)
  }

}

// ---- FROM helper --------------------------------------------------------------------------------

/**
 * Delegate rendering to each source's own `fromFragmentWith` kernel using the alias we captured when the source was
 * added. Since every `Relation` now carries its own alias, we could also call `entry.relation.fromFragment` — using the
 * explicit alias from the entry keeps the code honest about *which* alias is in scope (future self-joins of the same
 * relation value would want different aliases, and the entry is the source of truth).
 */
private[sharp] def aliasedFromEntry(s: SourceEntry[?, ?, ?, ?]): skunk.AppliedFragment =
  s.relation.fromFragmentWith(s.alias)

// ---- Join entry points -------------------------------------------------------------------------

/**
 * `.innerJoin` / `.leftJoin` / `.crossJoin` on any relation-like value — bare `Table` / `View` (auto-aliased to its own
 * name) or an already-aliased `Relation` from `.alias("…")` / `<sb>.alias("…")` / `<setOp>.alias("…")`.
 */
extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton](left: L)(using
  aL: AsRelation.Aux[L, RL, CL, AL]
) {

  /** `INNER JOIN` — right-side columns keep their declared types. */
  def innerJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR]
  ): IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, CR, AR] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL](aL, left)
    val rel       = aR(right)
    val rCols     = rel.columns.asInstanceOf[CR]
    new IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, CR, AR](
      baseEntry *: EmptyTuple,
      rel,
      aR.aliasValue(right),
      rCols,
      rCols,
      JoinKind.Inner
    )
  }

  /** `LEFT JOIN` — right-side column value types are wrapped in `Option`; `.opt` on the codecs at runtime. */
  def leftJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR]
  ): IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, NullableCols[CR], AR] = {
    val baseEntry    = makeBaseEntry[L, RL, CL, AL](aL, left)
    val rel          = aR(right)
    val origCols     = rel.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, NullableCols[CR], AR](
      baseEntry *: EmptyTuple,
      rel,
      aR.aliasValue(right),
      origCols,
      effectiveCls,
      JoinKind.Left
    )
  }

  /** `CROSS JOIN` — no predicate required; transitions straight to a two-source [[SelectBuilder]]. */
  def crossJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsRelation.Aux[R, RR, CR, AR]
  ): SelectBuilder[(SourceEntry[RL, CL, CL, AL], SourceEntry[RR, CR, CR, AR])] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL](aL, left)
    val rel       = aR(right)
    val rCols     = rel.columns.asInstanceOf[CR]
    val rEntry    =
      new SourceEntry[RR, CR, CR, AR](rel, aR.aliasValue(right), rCols, rCols, JoinKind.Cross, None)
    new SelectBuilder[(SourceEntry[RL, CL, CL, AL], SourceEntry[RR, CR, CR, AR])]((baseEntry, rEntry))
  }

}

private[sharp] def makeBaseEntry[L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton](
  aL: AsRelation.Aux[L, RL, CL, AL],
  left: L
): SourceEntry[RL, CL, CL, AL] = {
  val rel  = aL(left)
  val cols = rel.columns.asInstanceOf[CL]
  new SourceEntry[RL, CL, CL, AL](rel, aL.aliasValue(left), cols, cols, JoinKind.Inner, None)
}
