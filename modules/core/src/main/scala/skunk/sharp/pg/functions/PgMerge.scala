package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.TypedExpr

/** Functions only valid inside a `MERGE … RETURNING` list (PG 17+). */
trait PgMerge {

  /** `merge_action()` — `'INSERT'`, `'UPDATE'` or `'DELETE'`: which branch acted on the returned row. */
  val mergeAction: TypedExpr[String, Void] =
    TypedExpr(TypedExpr.voidFragment("merge_action()"), skunk.codec.all.text)

}
