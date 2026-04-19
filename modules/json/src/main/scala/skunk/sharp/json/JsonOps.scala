package skunk.sharp.json

import skunk.sharp.TypedExpr

/**
 * Extension operators on `TypedExpr[Jsonb]` — the Postgres jsonb operator set.
 *
 * The operators are defined as methods rather than cryptic symbols so they read well in a type-checked context:
 * `u.metadata.get("email")` is clearer than `u.metadata.->>("email")`, and doesn't clash with Scala's own symbolic
 * conventions. The original SQL operators appear only in the rendered output.
 *
 * Coverage:
 *
 *   - `get(key)` → `jsonb -> text` → Jsonb (get a field as jsonb; NULL if missing).
 *   - `getText(key)` → `jsonb ->> text` → String (get a field as text; NULL if missing).
 *   - `at(idx)` / `atText(idx)` — same but array index.
 *   - `path(keys*)` → `jsonb #> text[]` → Jsonb (walk a path).
 *   - `pathText(keys*)` → `jsonb #>> text[]` → String.
 *   - `contains(other)` → `jsonb @> jsonb` → Boolean.
 *   - `containedBy(other)` → `jsonb <@ jsonb` → Boolean.
 *   - `hasKey(key)` → `jsonb ? text` → Boolean.
 *   - `hasAnyKey(keys*)` / `hasAllKeys(keys*)` → `?|` / `?&`.
 */
extension (e: TypedExpr[Jsonb]) {

  /** `jsonb -> 'key'` — get a field as jsonb. */
  def get(key: String): TypedExpr[Jsonb] =
    TypedExpr(
      e.render |+| TypedExpr.raw(" -> ") |+| TypedExpr.lit(key).render,
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )

  /** `jsonb -> n` — get an array element as jsonb. */
  def at(idx: Int): TypedExpr[Jsonb] =
    TypedExpr(e.render |+| TypedExpr.raw(s" -> $idx"), summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec)

  /** `jsonb ->> 'key'` — get a field as text. */
  def getText(key: String): TypedExpr[String] =
    TypedExpr(e.render |+| TypedExpr.raw(" ->> ") |+| TypedExpr.lit(key).render, skunk.codec.all.text)

  /** `jsonb ->> n` — get an array element as text. */
  def atText(idx: Int): TypedExpr[String] =
    TypedExpr(e.render |+| TypedExpr.raw(s" ->> $idx"), skunk.codec.all.text)

  /** `jsonb #> '{a,b,c}'::text[]` — walk a path, return jsonb. */
  def path(keys: String*): TypedExpr[Jsonb] = {
    val arr = keys.map(escapePathElem).mkString("{", ",", "}")
    TypedExpr(
      e.render |+| TypedExpr.raw(" #> ") |+| TypedExpr.lit(arr).render |+| TypedExpr.raw("::text[]"),
      summon[skunk.sharp.pg.PgTypeFor[Jsonb]].codec
    )
  }

  /** `jsonb #>> '{a,b,c}'::text[]` — walk a path, return text. */
  def pathText(keys: String*): TypedExpr[String] = {
    val arr = keys.map(escapePathElem).mkString("{", ",", "}")
    TypedExpr(
      e.render |+| TypedExpr.raw(" #>> ") |+| TypedExpr.lit(arr).render |+| TypedExpr.raw("::text[]"),
      skunk.codec.all.text
    )
  }

  /** `jsonb @> jsonb` — left contains right. */
  def contains(other: TypedExpr[Jsonb]): TypedExpr[Boolean] =
    TypedExpr(e.render |+| TypedExpr.raw(" @> ") |+| other.render, skunk.codec.all.bool)

  /** `jsonb <@ jsonb` — left is contained by right. */
  def containedBy(other: TypedExpr[Jsonb]): TypedExpr[Boolean] =
    TypedExpr(e.render |+| TypedExpr.raw(" <@ ") |+| other.render, skunk.codec.all.bool)

  /** `jsonb ? 'key'` — does the top-level have the key? */
  def hasKey(key: String): TypedExpr[Boolean] =
    TypedExpr(e.render |+| TypedExpr.raw(" ? ") |+| TypedExpr.lit(key).render, skunk.codec.all.bool)

  /** `jsonb ?| ARRAY[...]` — does the top-level have any of the keys? */
  def hasAnyKey(keys: String*): TypedExpr[Boolean] =
    TypedExpr(
      e.render |+| TypedExpr.raw(s" ?| ${textArray(keys)}"),
      skunk.codec.all.bool
    )

  /** `jsonb ?& ARRAY[...]` — does the top-level have all the keys? */
  def hasAllKeys(keys: String*): TypedExpr[Boolean] =
    TypedExpr(
      e.render |+| TypedExpr.raw(s" ?& ${textArray(keys)}"),
      skunk.codec.all.bool
    )

  /** Render a `'{a,b,c}'`-style PG text path literal. */
  private def escapePathElem(s: String): String =
    // Escape backslash and double-quote; wrap in quotes only if needed.
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  /**
   * Render a Postgres text array literal `ARRAY['a', 'b']` inline — no bound params so hasAnyKey / hasAllKeys don't
   * need the arrays module.
   */
  private def textArray(keys: Seq[String]): String =
    keys.map(k => s"'${k.replace("'", "''")}'").mkString("ARRAY[", ", ", "]")

}
