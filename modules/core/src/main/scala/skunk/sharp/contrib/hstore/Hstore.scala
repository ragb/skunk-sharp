package skunk.sharp.contrib.hstore

import skunk.Codec
import skunk.data.Type
import skunk.sharp.pg.PgTypeFor

import scala.collection.mutable

/**
 * `hstore` — key/value store with text keys and nullable text values. Predates jsonb; still ubiquitous in legacy
 * schemas and fast for flat lookups.
 *
 * Requires `CREATE EXTENSION hstore;`.
 *
 * Wire format: `"k1"=>"v1", "k2"=>NULL, "k3"=>"v3"`. Both `encode` and `decode` are hand-rolled below — no external
 * library dependency. The decoder accepts the full Postgres output format (quoted keys/values, NULL literal,
 * backslash-escapes).
 */
opaque type Hstore <: Map[String, Option[String]] = Map[String, Option[String]]

object Hstore {

  val RequiredExtension: String = "hstore"

  def apply(entries: Map[String, Option[String]]): Hstore = entries
  def apply(entries: (String, Option[String])*): Hstore  = entries.toMap

  val codec: Codec[Hstore] =
    Codec.simple[Hstore](encode, decode(_).map(apply), Type("hstore"))

  given PgTypeFor[Hstore] = PgTypeFor.instanceWithExtension(codec, RequiredExtension)

  private def encode(h: Hstore): String = {
    val sb       = new StringBuilder
    var first    = true
    h.iterator.foreach { case (k, vOpt) =>
      if (first) first = false else sb ++= ", "
      sb += '"'
      escapeInto(k, sb)
      sb ++= "\"=>"
      vOpt match {
        case Some(v) =>
          sb += '"'
          escapeInto(v, sb)
          sb += '"'
        case None => sb ++= "NULL"
      }
    }
    sb.result()
  }

  private def escapeInto(s: String, sb: StringBuilder): Unit = {
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c == '"' || c == '\\') sb += '\\'
      sb += c
      i += 1
    }
  }

  private def decode(s: String): Either[String, Map[String, Option[String]]] = {
    val out = mutable.LinkedHashMap.empty[String, Option[String]]
    val len = s.length
    var i   = 0

    def skipWs(): Unit = while (i < len && s.charAt(i).isWhitespace) i += 1

    def parseQuoted(): Either[String, String] = {
      if (i >= len || s.charAt(i) != '"') Left(s"hstore: expected '\"' at offset $i")
      else {
        i += 1
        val buf = new StringBuilder
        var done = false
        var err: String = null
        while (!done && err == null) {
          if (i >= len) err = "hstore: unterminated quoted token"
          else {
            val c = s.charAt(i)
            if (c == '"') { done = true; i += 1 }
            else if (c == '\\') {
              if (i + 1 >= len) err = "hstore: dangling backslash"
              else { buf += s.charAt(i + 1); i += 2 }
            } else { buf += c; i += 1 }
          }
        }
        if (err != null) Left(err) else Right(buf.result())
      }
    }

    var result: Either[String, Unit] = Right(())
    while (result.isRight && { skipWs(); i < len }) {
      parseQuoted() match {
        case Left(e)  => result = Left(e)
        case Right(k) =>
          skipWs()
          if (i + 1 >= len || s.charAt(i) != '=' || s.charAt(i + 1) != '>')
            result = Left(s"hstore: expected '=>' after key at offset $i")
          else {
            i += 2
            skipWs()
            // Value is either NULL (case-insensitive, unquoted) or a quoted string.
            if (i + 3 < len && s.regionMatches(true, i, "NULL", 0, 4) &&
                (i + 4 == len || !isUnquotedTail(s.charAt(i + 4)))) {
              i += 4
              out.put(k, None)
            } else
              parseQuoted() match {
                case Left(e)  => result = Left(e)
                case Right(v) => out.put(k, Some(v))
              }
          }
          if (result.isRight) {
            skipWs()
            if (i < len) {
              if (s.charAt(i) != ',') result = Left(s"hstore: expected ',' at offset $i")
              else { i += 1; skipWs() }
            }
          }
      }
    }
    result.map(_ => out.toMap)
  }

  private def isUnquotedTail(c: Char): Boolean = c.isLetterOrDigit || c == '_'

}
