package skunk.sharp

/** Map `Cols` into a tuple of `TypedColumn`s — one per column, value type and nullability preserved. */
type TypedColumnsOf[Cols <: Tuple] <: Tuple = Cols match {
  case Column[t, n, nu, d] *: tail => TypedColumn[t, nu] *: TypedColumnsOf[tail]
  case EmptyTuple                  => EmptyTuple
}

/**
 * A named tuple carrying one [[TypedColumn]] per column, keyed by column name. This is the `cols` value that appears
 * inside WHERE/SELECT lambdas — `cols.email` resolves (via Scala 3.8 named-tuple Selectable support) to
 * `TypedColumn[String, false]` or whatever the declared types demand.
 */
type ColumnsView[Cols <: Tuple] = scala.NamedTuple.NamedTuple[NamesOf[Cols], TypedColumnsOf[Cols]]

object ColumnsView {

  /** Build the runtime view for a columns tuple. Named tuples are plain tuples at runtime, so a single cast is safe. */
  def apply[Cols <: Tuple](cols: Cols): ColumnsView[Cols] = {
    val typed: Array[Any] =
      cols.toList.map(c => TypedColumn.of(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]])).toArray[Any]
    Tuple.fromArray(typed).asInstanceOf[ColumnsView[Cols]]
  }

  /**
   * Build a [[ColumnsView]] whose columns render as `"qualifier"."colname"` — double-quoted alias prefix. Use for
   * user-provided relation aliases (`users.alias("u")` → `"u"."id"`).
   */
  def qualified[Cols <: Tuple](cols: Cols, qualifier: String): ColumnsView[Cols] = {
    val typed: Array[Any] =
      cols
        .toList
        .map(c => TypedColumn.qualified(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]], qualifier))
        .toArray[Any]
    Tuple.fromArray(typed).asInstanceOf[ColumnsView[Cols]]
  }

  /**
   * Same as [[qualified]] but leaves the qualifier bare (`qualifier."colname"`). Used for Postgres pseudo-tables like
   * `excluded` in `ON CONFLICT DO UPDATE` — a quoted `"excluded"` would be interpreted as a user identifier and fail to
   * reference the incoming-row pseudo-table.
   */
  def qualifiedRaw[Cols <: Tuple](cols: Cols, qualifier: String): ColumnsView[Cols] = {
    val typed: Array[Any] =
      cols
        .toList
        .map(c => TypedColumn.qualifiedRaw(c.asInstanceOf[Column[Any, "x", Boolean, Boolean]], qualifier))
        .toArray[Any]
    Tuple.fromArray(typed).asInstanceOf[ColumnsView[Cols]]
  }

}
