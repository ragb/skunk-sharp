package skunk.sharp.dsl

import skunk.Codec
import skunk.sharp.*
import skunk.sharp.where.Where

import scala.NamedTuple

/**
 * N-table SQL joins. `.innerJoin` / `.leftJoin` / `.crossJoin` chain off any relation (or [[AliasedRelation]]); each
 * join appends a [[SourceEntry]] to a `Sources <: Tuple` carried at the type level. The lambda passed to `.where` /
 * `.select` / `.orderBy` / `.groupBy` / `.having` receives a `JoinedView[Sources]` — a Scala 3 named tuple keyed by
 * alias — so columns are reached as `r.<alias>.<column>`.
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
 * LEFT JOIN flips the right-side columns' nullability for `.where` / `.select` — value types become `Option[T]` and
 * codecs get `.opt` at runtime. The `.on` predicate still sees the declared (non-nullable) types, because Postgres
 * evaluates `ON` before NULL-padding unmatched rows.
 *
 * `Select.from(a, b, ...)` (alias: `a.crossJoin(b)`) renders as `CROSS JOIN`, no `.on(...)` required.
 */

// ---- AliasedRelation + .alias extension --------------------------------------------------------

/** A relation paired with its SQL alias (`"u"`, `"p"`, `"sub"`, …). Constructed via the `.alias("u")` extension. */
final class AliasedRelation[R <: Relation[Cols], Cols <: Tuple, Alias <: String & Singleton] private[sharp] (
  val relation: R,
  val alias: Alias
)

/**
 * `.alias("u")` on any relation lifts it into an [[AliasedRelation]]. Named `.alias` (not `.as`) to keep a clean
 * separation from [[TypedExpr.as]], which renames a projected column in the SQL — a different operation.
 */
extension [R <: Relation[Cols], Cols <: Tuple](r: R) {

  def alias[Alias <: String & Singleton](a: Alias): AliasedRelation[R, Cols, Alias] =
    new AliasedRelation[R, Cols, Alias](r, a)

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

// ---- AsAliased typeclass -----------------------------------------------------------------------

/**
 * Evidence that `T` is, or can be implicitly wrapped as, an [[AliasedRelation]]. Three sources:
 *   - An already-aliased relation (identity).
 *   - A [[Table]] `Table[Cols, Name]` — auto-aliased to the table's name.
 *   - A [[View]] `View[Cols, Name]` — auto-aliased to the view's name.
 *
 * Lets `users.innerJoin(posts)` work without explicit `.alias(...)`: the alias defaults to the relation's name, pulled
 * from the `Name` type parameter so it's usable as a NamedTuple label in the join's lambda view (`r.users.id`).
 */
sealed trait AsAliased[T] {
  type R <: Relation[Cols]
  type Cols <: Tuple
  type Alias <: String & Singleton
  def apply(t: T): AliasedRelation[R, Cols, Alias]
}

object AsAliased {

  type Aux[T, R_ <: Relation[Cols_], Cols_ <: Tuple, Alias_ <: String & Singleton] = AsAliased[T] {
    type R     = R_
    type Cols  = Cols_
    type Alias = Alias_
  }

  given fromAliased[RR <: Relation[CC], CC <: Tuple, A <: String & Singleton]
    : AsAliased.Aux[AliasedRelation[RR, CC, A], RR, CC, A] = new AsAliased[AliasedRelation[RR, CC, A]] {
    type R     = RR
    type Cols  = CC
    type Alias = A
    def apply(a: AliasedRelation[RR, CC, A]): AliasedRelation[RR, CC, A] = a
  }

  given fromTable[CC <: Tuple, N <: String & Singleton]: AsAliased.Aux[Table[CC, N], Table[CC, N], CC, N] =
    new AsAliased[Table[CC, N]] {
      type R     = Table[CC, N]
      type Cols  = CC
      type Alias = N
      def apply(t: Table[CC, N]): AliasedRelation[Table[CC, N], CC, N] =
        new AliasedRelation[Table[CC, N], CC, N](t, t.name)
    }

  given fromView[CC <: Tuple, N <: String & Singleton]: AsAliased.Aux[View[CC, N], View[CC, N], CC, N] =
    new AsAliased[View[CC, N]] {
      type R     = View[CC, N]
      type Cols  = CC
      type Alias = N
      def apply(v: View[CC, N]): AliasedRelation[View[CC, N], CC, N] =
        new AliasedRelation[View[CC, N], CC, N](v, v.name)
    }

  /**
   * A derived [[SelectRelation]] — the result of `users.select.where(…).asRelation("active")` — auto-aliases to the
   * name supplied at construction. It already carries its own `fromFragment` (emitting `(<inner>) AS "<alias>"`), so
   * the rest of the join machinery treats it the same as a Table or View.
   */
  given fromSelectRelation[CC <: Tuple, N <: String & Singleton]
    : AsAliased.Aux[SelectRelation[CC, N], SelectRelation[CC, N], CC, N] =
    new AsAliased[SelectRelation[CC, N]] {
      type R     = SelectRelation[CC, N]
      type Cols  = CC
      type Alias = N
      def apply(r: SelectRelation[CC, N]): AliasedRelation[SelectRelation[CC, N], CC, N] =
        new AliasedRelation[SelectRelation[CC, N], CC, N](r, r.name)
    }

}

// ---- SourceEntry + match types over Sources ----------------------------------------------------

/**
 * A single source in an N-source join. Carries both the declared column tuple (`Cols0`) and the effective one (`Cols`, =
 * `NullableCols[Cols0]` when attached via LEFT JOIN). The lambda passed to `.where` / `.select` sees `Cols`; the `.on`
 * lambda for the just-added source sees `Cols0`.
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
 * `.compile` / `.select` here is a compile error because the method isn't here.
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

// ---- FROM helpers ------------------------------------------------------------------------------

/**
 * Render `"schema"."name" AS "alias"`, eliding the `AS` clause when the alias equals the relation's unqualified name
 * (the auto-alias case) — `"public"."posts"` already implies `"posts"` as the default alias.
 */
private[sharp] def aliasedFrom(ar: AliasedRelation[?, ?, ?]): skunk.AppliedFragment =
  ar.relation.fromFragment(ar.alias)

private[sharp] def aliasedFromEntry(s: SourceEntry[?, ?, ?, ?]): skunk.AppliedFragment =
  s.relation.fromFragment(s.alias)

// ---- Join entry points -------------------------------------------------------------------------

/**
 * `.innerJoin` / `.leftJoin` / `.crossJoin` on any relation-like value — bare `Table` / `View` (auto-aliased to its own
 * name) or an already-aliased `AliasedRelation`. The first chained call transitions from a bare relation to a
 * single-source [[JoinBuilder]], from which further sources can be attached.
 */
extension [L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton](left: L)(using
  aL: AsAliased.Aux[L, RL, CL, AL]
) {

  /** `INNER JOIN` — right-side columns keep their declared types. */
  def innerJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsAliased.Aux[R, RR, CR, AR]
  ): IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, CR, AR] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL](aL, left)
    val ar        = aR(right)
    val rCols     = ar.relation.columns.asInstanceOf[CR]
    new IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, CR, AR](
      baseEntry *: EmptyTuple,
      ar.relation,
      ar.alias,
      rCols,
      rCols,
      JoinKind.Inner
    )
  }

  /** `LEFT JOIN` — right-side column value types are wrapped in `Option`; `.opt` on the codecs at runtime. */
  def leftJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsAliased.Aux[R, RR, CR, AR]
  ): IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, NullableCols[CR], AR] = {
    val baseEntry    = makeBaseEntry[L, RL, CL, AL](aL, left)
    val ar           = aR(right)
    val origCols     = ar.relation.columns.asInstanceOf[CR]
    val effectiveCls = nullabilifyCols(origCols).asInstanceOf[NullableCols[CR]]
    new IncompleteJoin[SourceEntry[RL, CL, CL, AL] *: EmptyTuple, RR, CR, NullableCols[CR], AR](
      baseEntry *: EmptyTuple,
      ar.relation,
      ar.alias,
      origCols,
      effectiveCls,
      JoinKind.Left
    )
  }

  /** `CROSS JOIN` — no predicate required; transitions straight to a two-source [[SelectBuilder]]. */
  def crossJoin[R, RR <: Relation[CR], CR <: Tuple, AR <: String & Singleton](right: R)(using
    aR: AsAliased.Aux[R, RR, CR, AR]
  ): SelectBuilder[(SourceEntry[RL, CL, CL, AL], SourceEntry[RR, CR, CR, AR])] = {
    val baseEntry = makeBaseEntry[L, RL, CL, AL](aL, left)
    val ar        = aR(right)
    val rCols     = ar.relation.columns.asInstanceOf[CR]
    val rEntry    = new SourceEntry[RR, CR, CR, AR](ar.relation, ar.alias, rCols, rCols, JoinKind.Cross, None)
    new SelectBuilder[(SourceEntry[RL, CL, CL, AL], SourceEntry[RR, CR, CR, AR])]((baseEntry, rEntry))
  }

}

private[sharp] def makeBaseEntry[L, RL <: Relation[CL], CL <: Tuple, AL <: String & Singleton](
  aL: AsAliased.Aux[L, RL, CL, AL],
  left: L
): SourceEntry[RL, CL, CL, AL] = {
  val al   = aL(left)
  val cols = al.relation.columns.asInstanceOf[CL]
  new SourceEntry[RL, CL, CL, AL](al.relation, al.alias, cols, cols, JoinKind.Inner, None)
}
