package skunk.sharp.contrib.pgcrypto

import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/**
 * `pgcrypto` — server-side crypto primitives. Password hashing is the headline use case (`crypt` + `gen_salt`); also
 * provides `digest` and `hmac` for message digests over text.
 *
 * Requires `CREATE EXTENSION pgcrypto;`. This module has no tag types — every function returns a plain `String`, so the
 * validator can't auto-detect the dependency from column metadata. Pass `extraExtensions =
 * Set(PgCrypto.RequiredExtension)` to `SchemaValidator.validate` if you want validation coverage.
 *
 * Byte-array variants (`digest(bytea, algo)` → `bytea`) are deferred until core grows array / byte-array support.
 */
trait PgCrypto {

  /**
   * `crypt(password, salt)` → hashed text. With a freshly-generated `salt`, this produces a new hash; with an existing
   * stored hash as `salt`, the result equals the stored hash iff the password matches (canonical constant-time compare
   * idiom).
   */
  inline def crypt[A, B](
    password: TypedExpr[String, A],
    salt: TypedExpr[String, B]
  ): TypedExpr[String, Where.Concat[A, B]] =
    PgFunction.binary[String, String, String, A, B]("crypt")(password, salt)

  /** `gen_salt(algo)` — algorithms: `"bf"` (bcrypt), `"md5"`, `"xdes"`, `"des"`. Prefer `"bf"`. */
  inline def genSalt[A](algo: TypedExpr[String, A]): TypedExpr[String, A] =
    PgFunction.unary[String, String, A]("gen_salt")(algo)

  /** `gen_salt(algo, cost)` — bcrypt cost factor (typically 4..31). */
  inline def genSalt[A, B](
    algo: TypedExpr[String, A],
    cost: TypedExpr[Int, B]
  ): TypedExpr[String, Where.Concat[A, B]] =
    PgFunction.binary[String, Int, String, A, B]("gen_salt")(algo, cost)

  /**
   * `digest(data, algo)` → text. Algorithms: `"md5"`, `"sha1"`, `"sha224"`, `"sha256"`, `"sha384"`, `"sha512"`. The
   * String-only return form (`encode(digest(...), 'hex')`) is conventional — this binding renders the bare `digest`
   * call; wrap in `encode(_, 'hex')` if you want hex-text directly.
   */
  inline def digest[A, B](
    data: TypedExpr[String, A],
    algo: TypedExpr[String, B]
  ): TypedExpr[String, Where.Concat[A, B]] =
    PgFunction.binary[String, String, String, A, B]("digest")(data, algo)

  /** `hmac(data, key, algo)` — keyed hash. */
  inline def hmac[A, B, C](
    data: TypedExpr[String, A],
    key: TypedExpr[String, B],
    algo: TypedExpr[String, C]
  ): TypedExpr[String, Where.Concat[A, Where.Concat[B, C]]] = {
    val mid   = TypedExpr.combineSepInl[B, C](key.fragment, ", ", algo.fragment)
    val inner = TypedExpr.combineSepInl[A, Where.Concat[B, C]](data.fragment, ", ", mid)
    val frag  = TypedExpr.wrap("hmac(", inner, ")")
    TypedExpr[String, Where.Concat[A, Where.Concat[B, C]]](frag, skunk.codec.all.text)
  }

}

object PgCrypto extends PgCrypto {

  val RequiredExtension: String = "pgcrypto"
}
