package skunk.sharp.circe

import skunk.sharp.TypedExpr

/**
 * Scalar `jsonb` functions. Mix into a `Pg`-like namespace alongside the core traits:
 *
 * {{{
 *   object MyPg
 *     extends skunk.sharp.pg.functions.PgNumeric
 *     with skunk.sharp.pg.functions.PgString
 *     with skunk.sharp.json.PgJsonb
 * }}}
 *
 * Or ship your own `Jsonb` namespace (`object Jsonb extends PgJsonb`).
 *
 * Set-returning functions (`jsonb_array_elements`, `jsonb_object_keys`, `jsonb_path_query`, …) are intentionally
 * deferred — they belong in a FROM / JOIN position, which needs the join-as-relation machinery from issue #2.
 */
trait PgJsonb {

  /** `to_jsonb(expr)` — cast anything to jsonb. */
  def toJsonb[T](e: TypedExpr[T]): TypedExpr[Jsonb] =
    TypedExpr(
      TypedExpr.raw("to_jsonb(") |+| e.render |+| TypedExpr.raw(")"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )

  /** `jsonb_typeof(e)` — returns `'object'`, `'array'`, `'string'`, `'number'`, `'boolean'`, or `'null'`. */
  def jsonbTypeof(e: TypedExpr[Jsonb]): TypedExpr[String] =
    TypedExpr(TypedExpr.raw("jsonb_typeof(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.text)

  /** `jsonb_array_length(e)` — length of a jsonb array. */
  def jsonbArrayLength(e: TypedExpr[Jsonb]): TypedExpr[Int] =
    TypedExpr(TypedExpr.raw("jsonb_array_length(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.int4)

  /** `jsonb_strip_nulls(e)` — remove all object fields with null values. */
  def jsonbStripNulls(e: TypedExpr[Jsonb]): TypedExpr[Jsonb] =
    TypedExpr(
      TypedExpr.raw("jsonb_strip_nulls(") |+| e.render |+| TypedExpr.raw(")"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )

  /** `jsonb_pretty(e)` — human-readable indented text. */
  def jsonbPretty(e: TypedExpr[Jsonb]): TypedExpr[String] =
    TypedExpr(TypedExpr.raw("jsonb_pretty(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.text)

  /** `jsonb_set(target, path, new_value, create_if_missing)`. */
  def jsonbSet(
    target: TypedExpr[Jsonb],
    path: Seq[String],
    value: TypedExpr[Jsonb],
    createIfMissing: Boolean = true
  ): TypedExpr[Jsonb] = {
    val pathLit = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathArg = TypedExpr.lit(pathLit).render |+| TypedExpr.raw("::text[]")
    TypedExpr(
      TypedExpr.raw("jsonb_set(") |+| target.render |+| TypedExpr.raw(", ") |+|
        pathArg |+| TypedExpr.raw(", ") |+|
        value.render |+| TypedExpr.raw(s", $createIfMissing)"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )
  }

  /** `jsonb_insert(target, path, new_value, insert_after)` — insert without overwriting. */
  def jsonbInsert(
    target: TypedExpr[Jsonb],
    path: Seq[String],
    value: TypedExpr[Jsonb],
    insertAfter: Boolean = false
  ): TypedExpr[Jsonb] = {
    val pathLit = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathArg = TypedExpr.lit(pathLit).render |+| TypedExpr.raw("::text[]")
    TypedExpr(
      TypedExpr.raw("jsonb_insert(") |+| target.render |+| TypedExpr.raw(", ") |+|
        pathArg |+| TypedExpr.raw(", ") |+|
        value.render |+| TypedExpr.raw(s", $insertAfter)"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )
  }

  /** `jsonb` concatenation / merge: `a || b`. Unlike text `||`, this merges objects / appends arrays. */
  def jsonbConcat(a: TypedExpr[Jsonb], b: TypedExpr[Jsonb]): TypedExpr[Jsonb] =
    TypedExpr(
      a.render |+| TypedExpr.raw(" || ") |+| b.render,
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )

  /** `jsonb - 'key'` — delete a key from a jsonb object. */
  def jsonbDeleteKey(e: TypedExpr[Jsonb], key: String): TypedExpr[Jsonb] =
    TypedExpr(
      e.render |+| TypedExpr.raw(" - ") |+| TypedExpr.lit(key).render,
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )

}
