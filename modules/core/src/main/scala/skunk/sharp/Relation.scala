package skunk.sharp

import skunk.AppliedFragment

/**
 * Marker for whether a relation's alias was defaulted from its own identity (`Implicit` — Table/View using their name)
 * or supplied explicitly by the user (`Explicit` — via `.alias("x")`). Operations can require a specific mode in their
 * evidence — today used only to drive error-message wording, but wired as type members so future rules (e.g. "this
 * position needs an explicit alias") can plug in without a Relation-level refactor.
 */
sealed trait AliasMode

object AliasMode {
  sealed trait Implicit extends AliasMode
  sealed trait Explicit extends AliasMode
}

/**
 * A Postgres relation — anything that can appear in a FROM / JOIN position.
 *
 * Four kinds in practice:
 *   - [[Table]] — a `BASE TABLE`.
 *   - [[View]] — a `VIEW`.
 *   - A subquery promoted via `<selectBuilder>.alias("name")` — a derived relation.
 *   - (future) VALUES / set-returning functions — same shape, their own `fromFragmentWith` override.
 *
 * Every relation carries its own **alias** at the type level via the `Alias` type member, plus a [[AliasMode]] marker
 * saying how the alias was obtained:
 *   - [[AliasMode.Implicit]] — defaulted from the relation's own name (Tables / Views).
 *   - [[AliasMode.Explicit]] — supplied via `.alias("x")` on a Relation or a SelectBuilder.
 *
 * Both modes satisfy the JOIN machinery's "must have an alias" requirement. Positions that don't need an alias
 * (INSERT…SELECT, set-op operands, scalar subqueries via `.asExpr`) don't go through `AsRelation` at all — they take
 * queries directly via `AsSubquery`, so a bare `SelectBuilder` works there without any `.alias` ceremony. FROM / JOIN
 * position requires a `Relation` (and therefore an alias).
 *
 * The `Alias` singleton drives two things:
 *   - how the relation renders into FROM (`"users" AS "u"` for an explicit alias; bare `"users"` when the alias equals
 *     the relation's own name and no `AS` is needed);
 *   - the `NamedTuple` labels seen inside `JoinedView` for multi-source queries (`r.u.email`, `r.posts.title`).
 */
trait Relation[Cols <: Tuple] {

  /** Singleton alias type — carried so JOIN views can label named-tuple fields with the alias at compile time. */
  type Alias <: String & Singleton

  /** Implicit (from name) vs Explicit (from `.alias("x")`) — see [[AliasMode]]. */
  type Mode <: AliasMode

  /**
   * The alias value carried by this relation. Tables and views default it to their name; subqueries / re-aliased
   * wrappers carry the name given at creation. Named `currentAlias` (not just `alias`) so the no-arg member accessor
   * doesn't shadow the `.alias(name)` extension — `relation.alias` would otherwise resolve to this accessor and then
   * try to apply its `String` result to the argument, which is nonsensical.
   */
  def currentAlias: Alias

  /** The underlying relation identity — `"users"`, `"active_users"`, … — for schema lookup and qualifiedName. */
  def name: String
  def schema: Option[String]
  def columns: Cols

  /**
   * Whether this relation is a base table (`"BASE TABLE"`), a view (`"VIEW"`), or derived (`""`). Drives the schema
   * validator — derived relations (subqueries, VALUES, …) aren't registered in `information_schema` and are skipped.
   */
  def expectedTableType: String

  /**
   * `false` for the dedicated empty relation used to build FROM-less queries (`SELECT now()`); `true` for every real
   * relation. Drives whether the SELECT compiler emits a `FROM …` clause.
   */
  def hasFromClause: Boolean = true

  def qualifiedName: String =
    schema.fold(quoteIdent(name))(s => s"${quoteIdent(s)}.${quoteIdent(name)}")

  /**
   * **The rendering kernel** — emit SQL for this relation in a FROM position using the supplied alias string. Each kind
   * overrides this to match its shape:
   *
   *   - Table / View (default impl here): emit `"schema"."name"`, append `AS "alias"` only when `alias != name`.
   *   - Subquery: emit `(<inner SQL>) AS "alias"` (alias is mandatory per Postgres).
   *   - VALUES: emit `(VALUES (…)) AS "alias" (col1, col2)`.
   *
   * Returns `AppliedFragment` rather than `String` because derived kinds carry bound parameters from their inner SQL
   * (the `$N` placeholders need to thread through to the outer query's argument list); a plain String would drop them.
   *
   * A `String` is taken (not `self.alias`) so re-aliasing wrappers can pass their own alias into the underlying's
   * kernel without cloning rendering logic. The no-argument [[fromFragment]] below specialises to `self.alias`.
   */
  def fromFragmentWith(a: String): AppliedFragment = {
    val qn = qualifiedName
    if (a == name) TypedExpr.raw(qn)
    else TypedExpr.raw(s"""$qn AS "$a"""")
  }

  /** Convenience: render using this relation's own carried alias. The SELECT compiler calls this on each source. */
  final def fromFragment: AppliedFragment = fromFragmentWith(currentAlias)

  protected def quoteIdent(s: String): String = s""""$s""""
}

/**
 * The empty relation — no columns, no FROM clause. Use this to express queries that return only constants or
 * function-call results (`empty.select(_ => Pg.now)` → `SELECT now()`).
 *
 * Because its column tuple is `EmptyTuple`, `empty.select` with no explicit projection is useless (no columns to
 * project). The useful form is `empty.select(_ => <expr>)` or `empty.select(_ => (<e1>, <e2>))`.
 */
case object empty extends Relation[EmptyTuple] {
  type Alias = ""
  type Mode  = AliasMode.Implicit
  val currentAlias: ""                = ""
  val name: String                    = ""
  val schema: Option[String]          = None
  val columns: EmptyTuple             = EmptyTuple
  val expectedTableType: String       = ""
  override val hasFromClause: Boolean = false
  override val qualifiedName: String  = ""
}
