package skunk.sharp

/**
 * A named, column-typed Postgres relation — base class for both [[Table]] (read/write) and [[View]] (read-only).
 *
 * Everything that can be queried is a `Relation`: SELECT is defined once as an extension on `Relation`. Mutations
 * (INSERT, UPDATE, DELETE) live as extensions on [[Table]] alone, so attempting to mutate a [[View]] is a compile
 * error.
 */
trait Relation[Cols <: Tuple] {

  /**
   * Path-dependent singleton of the relation's name — subclasses override with `type Name = name.type` so every
   * instance carries its name in the type system. JOIN extensions use this to default the alias to the relation's name
   * when the user didn't supply an explicit `.alias(…)`.
   */
  type Name <: String & Singleton

  def name: String
  def schema: Option[String]
  def columns: Cols

  /**
   * Whether this relation is a base table (`"BASE TABLE"`) or a view (`"VIEW"`). Matches
   * `information_schema.tables.table_type`, so the schema validator can check the kind as well as the columns.
   */
  def expectedTableType: String

  /**
   * `false` for the dedicated empty relation used to build FROM-less queries (`SELECT now()`); `true` for real tables
   * and views. Drives whether the SELECT compiler emits a `FROM …` clause.
   */
  def hasFromClause: Boolean = true

  def qualifiedName: String =
    schema.fold(quoteIdent(name))(s => s"${quoteIdent(s)}.${quoteIdent(name)}")

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
  type Name = ""
  val name: String                    = ""
  val schema: Option[String]          = None
  val columns: EmptyTuple             = EmptyTuple
  val expectedTableType: String       = ""
  override val hasFromClause: Boolean = false
  override val qualifiedName: String  = ""
}
