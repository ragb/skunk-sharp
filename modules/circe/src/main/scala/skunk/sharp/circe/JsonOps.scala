package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.{Fragment, Void}
import skunk.sharp.{Param, PgOperator, TypedExpr}
import skunk.sharp.where.Where

/**
 * Extension operators on `TypedExpr[Jsonb[A], X]`. Args of input expression(s) propagate.
 */
extension [A, X](e: TypedExpr[Jsonb[A], X]) {

  /** `jsonb -> 'key'` — get a field as jsonb. */
  inline def get(key: String)(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[Jsonb[CirceJson], X] = {
    val keyFrag = Param.bind[String](key).fragment
    val frag    =
      PgOperator.binary[X, Void]("->", e.fragment, keyFrag).asInstanceOf[Fragment[X]]
    TypedExpr[Jsonb[CirceJson], X](frag, summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec)
  }

  /** `jsonb -> n` — get an array element as jsonb. */
  def at(idx: Int): TypedExpr[Jsonb[CirceJson], X] = {
    val parts = Left("(") :: e.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s" -> $idx)"))
    val frag  = Fragment[X](parts, e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[Jsonb[CirceJson], X](frag, summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec)
  }

  /** `jsonb ->> 'key'` — get a field as text. */
  inline def getText(key: String)(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[String, X] = {
    val keyFrag = Param.bind[String](key).fragment
    val frag    =
      PgOperator.binary[X, Void]("->>", e.fragment, keyFrag).asInstanceOf[Fragment[X]]
    TypedExpr[String, X](frag, skunk.codec.all.text)
  }

  /** `jsonb ->> n` — get an array element as text. */
  def atText(idx: Int): TypedExpr[String, X] = {
    val parts = Left("(") :: e.fragment.parts ++ List[Either[String, cats.data.State[Int, String]]](Left(s" ->> $idx)"))
    val frag  = Fragment[X](parts, e.fragment.encoder, skunk.util.Origin.unknown)
    TypedExpr[String, X](frag, skunk.codec.all.text)
  }

  /** `jsonb #> '{a,b,c}'::text[]` — walk a path, return jsonb. */
  inline def path(keys: String*)(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[Jsonb[CirceJson], X] = {
    val arr                      = keys.map(escapePathElem).mkString("{", ",", "}")
    val arrFrag                  = Param.bind[String](arr).fragment
    val withCast: Fragment[Void] = {
      val parts = arrFrag.parts ++ List[Either[String, cats.data.State[Int, String]]](Left("::text[]"))
      Fragment(parts, arrFrag.encoder, skunk.util.Origin.unknown)
    }
    val frag =
      PgOperator.binary[X, Void]("#>", e.fragment, withCast).asInstanceOf[Fragment[X]]
    TypedExpr[Jsonb[CirceJson], X](frag, summon[skunk.sharp.pg.PgTypeFor[Jsonb[CirceJson]]].codec)
  }

  /** `jsonb #>> '{a,b,c}'::text[]` — walk a path, return text. */
  inline def pathText(keys: String*)(using pfs: skunk.sharp.pg.PgTypeFor[String]): TypedExpr[String, X] = {
    val arr                      = keys.map(escapePathElem).mkString("{", ",", "}")
    val arrFrag                  = Param.bind[String](arr).fragment
    val withCast: Fragment[Void] = {
      val parts = arrFrag.parts ++ List[Either[String, cats.data.State[Int, String]]](Left("::text[]"))
      Fragment(parts, arrFrag.encoder, skunk.util.Origin.unknown)
    }
    val frag = PgOperator.binary[X, Void]("#>>", e.fragment, withCast).asInstanceOf[Fragment[X]]
    TypedExpr[String, X](frag, skunk.codec.all.text)
  }

  /** `jsonb @> jsonb` — left contains right. Args propagate from both sides. */
  inline def contains[B, Y](other: TypedExpr[Jsonb[B], Y]): Where[Where.Concat[X, Y]] = {
    Where(PgOperator.binary[X, Y]("@>", e.fragment, other.fragment))
  }

  /** `jsonb <@ jsonb`. */
  inline def containedBy[B, Y](other: TypedExpr[Jsonb[B], Y]): Where[Where.Concat[X, Y]] = {
    Where(PgOperator.binary[X, Y]("<@", e.fragment, other.fragment))
  }

  /** `jsonb ? 'key'` — does the top-level have the key? */
  inline def hasKey(key: String)(using pfs: skunk.sharp.pg.PgTypeFor[String]): Where[X] = {
    val keyFrag = Param.bind[String](key).fragment
    val frag    =
      PgOperator.binary[X, Void]("?", e.fragment, keyFrag).asInstanceOf[Fragment[X]]
    Where(frag)
  }

  /** `jsonb ?| ARRAY[...]`. */
  def hasAnyKey(keys: String*): Where[X] = {
    val parts = Left("(") :: e.fragment.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(s" ?| ${textArray(keys)})"))
    val frag = Fragment[X](parts, e.fragment.encoder, skunk.util.Origin.unknown)
    Where(frag)
  }

  /** `jsonb ?& ARRAY[...]`. */
  def hasAllKeys(keys: String*): Where[X] = {
    val parts = Left("(") :: e.fragment.parts ++
      List[Either[String, cats.data.State[Int, String]]](Left(s" ?& ${textArray(keys)})"))
    val frag = Fragment[X](parts, e.fragment.encoder, skunk.util.Origin.unknown)
    Where(frag)
  }

  private def escapePathElem(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"")

  private def textArray(keys: Seq[String]): String =
    keys.map(k => s"'${k.replace("'", "''")}'").mkString("ARRAY[", ", ", "]")

}
