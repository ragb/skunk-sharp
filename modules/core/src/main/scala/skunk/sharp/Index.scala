package skunk.sharp

/**
 * Sort order of one index key. Postgres's defaults are `ASC NULLS LAST` and `DESC NULLS FIRST`; the other two
 * combinations are the explicit `NULLS FIRST` / `NULLS LAST` variants.
 */
final case class IndexOrder(desc: Boolean, nullsFirst: Boolean) {

  /** The key's modifiers as Postgres prints them in an index definition (`""`, `" DESC"`, `" NULLS FIRST"`, …). */
  def sql: String = (desc, nullsFirst) match {
    case (false, false) => ""
    case (false, true)  => " NULLS FIRST"
    case (true, true)   => " DESC"
    case (true, false)  => " DESC NULLS LAST"
  }

}

object IndexOrder {
  val Asc: IndexOrder           = IndexOrder(desc = false, nullsFirst = false)
  val Desc: IndexOrder          = IndexOrder(desc = true, nullsFirst = true)
  val AscNullsFirst: IndexOrder = IndexOrder(desc = false, nullsFirst = true)
  val DescNullsLast: IndexOrder = IndexOrder(desc = true, nullsFirst = false)
}

/**
 * One key of an index: a column, or an expression (as SQL text), with an optional collation, operator class and sort
 * order.
 *
 * Expressions are compared with Postgres's own rendering of them, so write them the way `pg_get_indexdef` prints them
 * (e.g. `lower((email)::text)` rather than `lower(email)`) — a mismatch report shows the database's form to copy.
 */
final case class IndexKey(
  sql: String,
  isColumn: Boolean,
  order: IndexOrder = IndexOrder.Asc,
  opclass: Option[String] = None,
  collation: Option[String] = None
) {
  def desc: IndexKey                   = copy(order = IndexOrder.Desc)
  def descNullsLast: IndexKey          = copy(order = IndexOrder.DescNullsLast)
  def ascNullsFirst: IndexKey          = copy(order = IndexOrder.AscNullsFirst)
  def ordered(o: IndexOrder): IndexKey = copy(order = o)
  def opclass(name: String): IndexKey  = copy(opclass = Some(name))
  def collate(name: String): IndexKey  = copy(collation = Some(name))

  /** As Postgres prints a key: `<expr> [COLLATE "c"] [opclass] [DESC …]`. */
  def definition: String =
    (if (isColumn) sql else s"($sql)") + collation.fold("")(c => s""" COLLATE "$c"""") +
      opclass.fold("")(o => s" $o") + order.sql

}

object IndexKey {

  /** A column key — its name is checked against the table's columns when the index is declared. */
  def column(name: String): IndexKey = IndexKey(name, isColumn = true)

  /** An expression key, as SQL text (not checked — see [[IndexKey]] on how it is compared). */
  def expr(sql: String): IndexKey = IndexKey(sql, isColumn = false)
}

/**
 * A declared index — see [[Table.withIndex]] / [[Table.withIndexDef]]. Declaration only (the DSL never generates DDL):
 * the schema validator compares it with Postgres's own `pg_get_indexdef`.
 *
 * {{{
 *   IndexDef("chunks_embedding_hnsw", IndexKey.column("embedding").opclass("vector_cosine_ops"))
 *     .withMethod("hnsw")
 *     .withStorage("m" -> "16", "ef_construction" -> "64")
 * }}}
 */
final case class IndexDef(
  name: String,
  keys: List[IndexKey],
  predicate: Option[String] = None,
  method: String = "btree",
  includes: List[String] = Nil,
  storage: List[(String, String)] = Nil,
  isUnique: Boolean = false
) {
  def withMethod(method: String): IndexDef             = copy(method = method)
  def include(columns: String*): IndexDef              = copy(includes = includes ++ columns)
  def where(sql: String): IndexDef                     = copy(predicate = Some(sql))
  def withStorage(params: (String, String)*): IndexDef = copy(storage = storage ++ params)

  /** A standalone `CREATE UNIQUE INDEX` (UNIQUE *constraints* are declared with `withUnique` / `withUniqueIndex`). */
  def unique: IndexDef = copy(isUnique = true)

  /** Normalised definition, in the shape Postgres prints from `USING` on (prefixed `UNIQUE` for unique indexes). */
  def definition: String =
    (if (isUnique) "UNIQUE " else "") +
      keys.map(_.definition).mkString(s"USING $method (", ", ", ")") +
      (if (includes.isEmpty) "" else includes.mkString(" INCLUDE (", ", ", ")")) +
      (if (storage.isEmpty) "" else storage.map((k, v) => s"$k='$v'").mkString(" WITH (", ", ", ")")) +
      predicate.fold("")(w => s" WHERE $w")

}

object IndexDef {

  /** An index on `keys`, btree unless changed with `.withMethod`. */
  def apply(name: String, key: IndexKey, more: IndexKey*): IndexDef = IndexDef(name, key :: more.toList)
}
