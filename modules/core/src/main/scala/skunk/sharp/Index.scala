package skunk.sharp

/**
 * Sort order of one index key column. Postgres's defaults are `ASC NULLS LAST` and `DESC NULLS FIRST`; the other two
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
 * A declared non-unique btree index — see [[Table.withIndex]]. Declaration only (the DSL never generates DDL): the
 * schema validator diffs it against `pg_index`.
 */
final case class IndexDef(name: String, columns: List[(String, IndexOrder)], where: Option[String]) {

  /** Normalised definition, in the same shape the validator renders the database's side. */
  def definition: String =
    columns.map((c, o) => c + o.sql).mkString("btree (", ", ", ")") + where.fold("")(w => s" WHERE $w")

}
