package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.sharp.TypedExpr

/**
 * Scalar `jsonb` functions. Mix into a `Pg`-like namespace alongside the core traits:
 *
 * {{{
 *   object MyPg
 *     extends skunk.sharp.pg.functions.PgNumeric
 *     with skunk.sharp.pg.functions.PgString
 *     with skunk.sharp.circe.PgJsonb
 * }}}
 *
 * Or just use the ready-made `Jsonb` namespace — `object Jsonb extends PgJsonb` (see [[tags]]).
 *
 * Every jsonb argument accepts any `TypedExpr[Jsonb[A]]` for any `A`; return types widen to `Jsonb[io.circe.Json]`
 * because the result's static shape isn't knowable in general (a field merge, path set, or key delete can produce
 * arbitrary JSON).
 *
 * Set-returning functions (`jsonb_array_elements`, `jsonb_object_keys`, `jsonb_path_query`, …) are intentionally
 * deferred — they belong in a FROM / JOIN position, which needs the join-as-relation machinery from issue #2.
 */
trait PgJsonb {

  private inline def rawJsonbCodec =
    summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec

  /** `to_jsonb(expr)` — cast anything to jsonb. */
  def toJsonb[T](e: TypedExpr[T]): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(TypedExpr.raw("to_jsonb(") |+| e.render |+| TypedExpr.raw(")"), rawJsonbCodec)

  /** `jsonb_typeof(e)` — returns `'object'`, `'array'`, `'string'`, `'number'`, `'boolean'`, or `'null'`. */
  def jsonbTypeof[A](e: TypedExpr[Jsonb[A]]): TypedExpr[String] =
    TypedExpr(TypedExpr.raw("jsonb_typeof(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.text)

  /** `jsonb_array_length(e)` — length of a jsonb array. */
  def jsonbArrayLength[A](e: TypedExpr[Jsonb[A]]): TypedExpr[Int] =
    TypedExpr(TypedExpr.raw("jsonb_array_length(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.int4)

  /** `jsonb_strip_nulls(e)` — remove all object fields with null values. */
  def jsonbStripNulls[A](e: TypedExpr[Jsonb[A]]): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(
      TypedExpr.raw("jsonb_strip_nulls(") |+| e.render |+| TypedExpr.raw(")"),
      rawJsonbCodec
    )

  /** `jsonb_pretty(e)` — human-readable indented text. */
  def jsonbPretty[A](e: TypedExpr[Jsonb[A]]): TypedExpr[String] =
    TypedExpr(TypedExpr.raw("jsonb_pretty(") |+| e.render |+| TypedExpr.raw(")"), skunk.codec.all.text)

  /** `jsonb_set(target, path, new_value, create_if_missing)`. */
  def jsonbSet[A, B](
    target: TypedExpr[Jsonb[A]],
    path: Seq[String],
    value: TypedExpr[Jsonb[B]],
    createIfMissing: Boolean = true
  ): TypedExpr[Jsonb[CirceJson]] = {
    val pathLit = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathArg = TypedExpr.lit(pathLit).render |+| TypedExpr.raw("::text[]")
    TypedExpr(
      TypedExpr.raw("jsonb_set(") |+| target.render |+| TypedExpr.raw(", ") |+|
        pathArg |+| TypedExpr.raw(", ") |+|
        value.render |+| TypedExpr.raw(s", $createIfMissing)"),
      rawJsonbCodec
    )
  }

  /** `jsonb_insert(target, path, new_value, insert_after)` — insert without overwriting. */
  def jsonbInsert[A, B](
    target: TypedExpr[Jsonb[A]],
    path: Seq[String],
    value: TypedExpr[Jsonb[B]],
    insertAfter: Boolean = false
  ): TypedExpr[Jsonb[CirceJson]] = {
    val pathLit = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathArg = TypedExpr.lit(pathLit).render |+| TypedExpr.raw("::text[]")
    TypedExpr(
      TypedExpr.raw("jsonb_insert(") |+| target.render |+| TypedExpr.raw(", ") |+|
        pathArg |+| TypedExpr.raw(", ") |+|
        value.render |+| TypedExpr.raw(s", $insertAfter)"),
      rawJsonbCodec
    )
  }

  /** `jsonb` concatenation / merge: `a || b`. Unlike text `||`, this merges objects / appends arrays. */
  def jsonbConcat[A, B](a: TypedExpr[Jsonb[A]], b: TypedExpr[Jsonb[B]]): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(a.render |+| TypedExpr.raw(" || ") |+| b.render, rawJsonbCodec)

  /** `jsonb - 'key'` — delete a key from a jsonb object. */
  def jsonbDeleteKey[A](e: TypedExpr[Jsonb[A]], key: String): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(
      e.render |+| TypedExpr.raw(" - ") |+| TypedExpr.lit(key).render,
      rawJsonbCodec
    )

}
