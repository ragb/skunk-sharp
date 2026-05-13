package skunk.sharp.contrib.hstore

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/** Function helpers for the `hstore` extension. */
trait PgHstore {

  /** `hstore_to_json(h)` — render hstore as a JSON object. Returns text (parse downstream if needed). */
  inline def hstoreToJson[A](h: TypedExpr[Hstore, A]): TypedExpr[String, A] =
    PgFunction.unary[Hstore, String, A]("hstore_to_json")(h)

  /** `defined(h, key)` — `true` iff `key` exists *and* its value is non-NULL. */
  inline def defined[A, B](
    h: TypedExpr[Hstore, A],
    key: TypedExpr[String, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Hstore, String, Boolean, A, B]("defined")(h, key)

}

object PgHstore extends PgHstore
