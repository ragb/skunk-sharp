package skunk.sharp.pg.functions

import skunk.sharp.TypedExpr

/**
 * Session / connection introspection keywords — SQL-standard value functions that take no parentheses (`CURRENT_USER`,
 * `SESSION_USER`, …) plus Postgres's `current_database()` (parenthesised, despite naming).
 *
 * All render inline as the keyword itself; decoders are `text` / `varchar`. Useful for audit columns, RLS predicates
 * (`created_by = current_user`), diagnostic SELECTs, and multi-tenant defaults.
 */
trait PgSession {

  /** `current_user` — the effective role id in force for the session. */
  val currentUser: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("current_user"), skunk.codec.all.text)

  /** `session_user` — the login role, unaffected by `SET ROLE`. */
  val sessionUser: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("session_user"), skunk.codec.all.text)

  /** `user` — SQL-standard alias of `current_user`. */
  val user: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("user"), skunk.codec.all.text)

  /** `current_schema` — first schema in the current `search_path`. */
  val currentSchema: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("current_schema"), skunk.codec.all.text)

  /** `current_catalog` — SQL-standard synonym for the current database name (Postgres extension). */
  val currentCatalog: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("current_catalog"), skunk.codec.all.text)

  /** `current_database()` — the current database name. Parenthesised despite the naming. */
  val currentDatabase: TypedExpr[String] =
    TypedExpr(TypedExpr.raw("current_database()"), skunk.codec.all.text)

}
