package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.sharp.TypedExpr
import skunk.sharp.where.Where

/**
 * Extension operators on `TypedExpr[Jsonb[A]]` for any `A` — the operators act at the SQL level on jsonb bytes, so the
 * static type parameter doesn't influence rendering. When a navigation step loses the static shape (e.g.
 * `.get("someField")`), the result widens to `Jsonb[io.circe.Json]`.
 *
 * Named methods rather than cryptic symbols — `u.metadata.get("email")` is clearer than `u.metadata.->>("email")`, and
 * doesn't clash with Scala's own symbolic conventions. The original SQL operators appear only in the rendered output.
 */
extension [A](e: TypedExpr[Jsonb[A]]) {

  /** `jsonb -> 'key'` — get a field as jsonb (raw JSON; shape unknown after navigation). */
  def get(key: String): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(
      e.render |+| TypedExpr.raw(" -> ") |+| TypedExpr.parameterised(key).render,
      summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec
    )

  /** `jsonb -> n` — get an array element as jsonb. */
  def at(idx: Int): TypedExpr[Jsonb[CirceJson]] =
    TypedExpr(
      e.render |+| TypedExpr.raw(s" -> $idx"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec
    )

  /** `jsonb ->> 'key'` — get a field as text. */
  def getText(key: String): TypedExpr[String] =
    TypedExpr(e.render |+| TypedExpr.raw(" ->> ") |+| TypedExpr.parameterised(key).render, skunk.codec.all.text)

  /** `jsonb ->> n` — get an array element as text. */
  def atText(idx: Int): TypedExpr[String] =
    TypedExpr(e.render |+| TypedExpr.raw(s" ->> $idx"), skunk.codec.all.text)

  /** `jsonb #> '{a,b,c}'::text[]` — walk a path, return jsonb. */
  def path(keys: String*): TypedExpr[Jsonb[CirceJson]] = {
    val arr = keys.map(escapePathElem).mkString("{", ",", "}")
    TypedExpr(
      e.render |+| TypedExpr.raw(" #> ") |+| TypedExpr.parameterised(arr).render |+| TypedExpr.raw("::text[]"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec
    )
  }

  /** `jsonb #>> '{a,b,c}'::text[]` — walk a path, return text. */
  def pathText(keys: String*): TypedExpr[String] = {
    val arr = keys.map(escapePathElem).mkString("{", ",", "}")
    TypedExpr(
      e.render |+| TypedExpr.raw(" #>> ") |+| TypedExpr.parameterised(arr).render |+| TypedExpr.raw("::text[]"),
      skunk.codec.all.text
    )
  }

  /** `jsonb @> jsonb` — left contains right. */
  def contains[B](other: TypedExpr[Jsonb[B]]): Where[skunk.Void] =
    Where(TypedExpr(e.render |+| TypedExpr.raw(" @> ") |+| other.render, skunk.codec.all.bool))

  /** `jsonb <@ jsonb` — left is contained by right. */
  def containedBy[B](other: TypedExpr[Jsonb[B]]): Where[skunk.Void] =
    Where(TypedExpr(e.render |+| TypedExpr.raw(" <@ ") |+| other.render, skunk.codec.all.bool))

  /** `jsonb ? 'key'` — does the top-level have the key? */
  def hasKey(key: String): Where[skunk.Void] =
    Where(TypedExpr(e.render |+| TypedExpr.raw(" ? ") |+| TypedExpr.parameterised(key).render, skunk.codec.all.bool))

  /** `jsonb ?| ARRAY[...]` — does the top-level have any of the keys? */
  def hasAnyKey(keys: String*): Where[skunk.Void] =
    Where(TypedExpr(e.render |+| TypedExpr.raw(s" ?| ${textArray(keys)}"), skunk.codec.all.bool))

  /** `jsonb ?& ARRAY[...]` — does the top-level have all the keys? */
  def hasAllKeys(keys: String*): Where[skunk.Void] =
    Where(TypedExpr(e.render |+| TypedExpr.raw(s" ?& ${textArray(keys)}"), skunk.codec.all.bool))

  /** Render a `'{a,b,c}'`-style PG text path literal. */
  private def escapePathElem(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  /**
   * Render a Postgres text array literal `ARRAY['a', 'b']` inline — no bound params so `hasAnyKey` / `hasAllKeys` don't
   * need the arrays module.
   */
  private def textArray(keys: Seq[String]): String =
    keys.map(k => s"'${k.replace("'", "''")}'").mkString("ARRAY[", ", ", "]")

}
