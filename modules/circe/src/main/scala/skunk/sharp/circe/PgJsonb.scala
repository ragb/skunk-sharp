package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.{Fragment, Void}
import skunk.sharp.{Param, TypedExpr}
import skunk.sharp.where.Where

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
 * Args of input expression(s) propagate to the result.
 */
trait PgJsonb {

  private inline def rawJsonbCodec =
    summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec

  /** `to_jsonb(expr)` — cast anything to jsonb. */
  def toJsonb[T, X](e: TypedExpr[T, X]): TypedExpr[Jsonb[CirceJson], X] = {
    val frag = TypedExpr.wrap("to_jsonb(", e.fragment, ")")
    TypedExpr[Jsonb[CirceJson], X](frag, rawJsonbCodec)
  }

  def jsonbTypeof[A, X](e: TypedExpr[Jsonb[A], X]): TypedExpr[String, X] = {
    val frag = TypedExpr.wrap("jsonb_typeof(", e.fragment, ")")
    TypedExpr[String, X](frag, skunk.codec.all.text)
  }

  def jsonbArrayLength[A, X](e: TypedExpr[Jsonb[A], X]): TypedExpr[Int, X] = {
    val frag = TypedExpr.wrap("jsonb_array_length(", e.fragment, ")")
    TypedExpr[Int, X](frag, skunk.codec.all.int4)
  }

  def jsonbStripNulls[A, X](e: TypedExpr[Jsonb[A], X]): TypedExpr[Jsonb[CirceJson], X] = {
    val frag = TypedExpr.wrap("jsonb_strip_nulls(", e.fragment, ")")
    TypedExpr[Jsonb[CirceJson], X](frag, rawJsonbCodec)
  }

  def jsonbPretty[A, X](e: TypedExpr[Jsonb[A], X]): TypedExpr[String, X] = {
    val frag = TypedExpr.wrap("jsonb_pretty(", e.fragment, ")")
    TypedExpr[String, X](frag, skunk.codec.all.text)
  }

  /**
   * `jsonb_set(target, path, new_value, create_if_missing)`. Args = Void — variadic-shaped builder using
   * [[TypedExpr.joinedVoid]] to thread the encoder via Concat2.bothVoid contramap chain.
   */
  def jsonbSet[A, B](
    target: TypedExpr[Jsonb[A], ?],
    path: Seq[String],
    value: TypedExpr[Jsonb[B], ?],
    createIfMissing: Boolean = true
  )(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[Jsonb[CirceJson], Void] = {
    val pathLit  = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathFrag = appendCast(Param.bind[String](pathLit).fragment, "::text[]")
    val argsFrag = TypedExpr.joinedVoid(", ", List(target.fragment, pathFrag, value.fragment))
    val parts = List[Either[String, cats.data.State[Int, String]]](Left("jsonb_set(")) ++ argsFrag.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(s", $createIfMissing)"))
    val frag  = Fragment(parts, argsFrag.encoder, skunk.util.Origin.unknown)
    TypedExpr[Jsonb[CirceJson], Void](frag, rawJsonbCodec)
  }

  def jsonbInsert[A, B](
    target: TypedExpr[Jsonb[A], ?],
    path: Seq[String],
    value: TypedExpr[Jsonb[B], ?],
    insertAfter: Boolean = false
  )(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[Jsonb[CirceJson], Void] = {
    val pathLit  = path.map(p => p.replace("\\", "\\\\").replace("\"", "\\\"")).mkString("{", ",", "}")
    val pathFrag = appendCast(Param.bind[String](pathLit).fragment, "::text[]")
    val argsFrag = TypedExpr.joinedVoid(", ", List(target.fragment, pathFrag, value.fragment))
    val parts = List[Either[String, cats.data.State[Int, String]]](Left("jsonb_insert(")) ++ argsFrag.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(s", $insertAfter)"))
    val frag  = Fragment(parts, argsFrag.encoder, skunk.util.Origin.unknown)
    TypedExpr[Jsonb[CirceJson], Void](frag, rawJsonbCodec)
  }

  private def appendCast(f: Fragment[Void], cast: String): Fragment[Void] = {
    val parts = f.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(cast))
    Fragment(parts, f.encoder, skunk.util.Origin.unknown)
  }

  /** `jsonb` concatenation / merge: `a || b`. */
  def jsonbConcat[A, B, X, Y](
    a: TypedExpr[Jsonb[A], X], b: TypedExpr[Jsonb[B], Y]
  ): TypedExpr[Jsonb[CirceJson], Where.Concat[X, Y]] = {
    val frag = TypedExpr.combineSep(a.fragment, " || ", b.fragment)
    TypedExpr[Jsonb[CirceJson], Where.Concat[X, Y]](frag, rawJsonbCodec)
  }

  /** `jsonb - 'key'` — delete a key from a jsonb object. */
  def jsonbDeleteKey[A, X](e: TypedExpr[Jsonb[A], X], key: String)(using
    pfs: skunk.sharp.pg.PgTypeFor[String]
  ): TypedExpr[Jsonb[CirceJson], X] = {
    val keyFrag = Param.bind[String](key).fragment
    val frag    = TypedExpr.combineSep(e.fragment, " - ", keyFrag).asInstanceOf[Fragment[X]]
    TypedExpr[Jsonb[CirceJson], X](frag, rawJsonbCodec)
  }

}
