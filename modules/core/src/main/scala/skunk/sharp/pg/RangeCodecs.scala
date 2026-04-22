// Copyright 2026 Rui Batista
//
// SPDX-License-Identifier: Apache-2.0

package skunk.sharp.pg

import skunk.Codec
import skunk.sharp.data.Range

/**
 * Internal helpers for building Postgres range codecs from an inner element codec.
 *
 * Postgres range literals: `[lower,upper)`, `(lower,upper]`, `[,upper)` (unbounded lower), `(,)` (all), `empty`.
 * Values that contain spaces, commas, or brackets are double-quoted by Postgres; we handle this in both encoding and
 * decoding.
 */
private[pg] object RangeCodecs {

  /**
   * Build a `Codec[Range[A]]` from an inner scalar codec and the Postgres type name.
   *
   * Encoding: serialises to Postgres range literal text, quoting element values that contain spaces or special chars.
   * Decoding: parses the range literal, strips quotes, then delegates each element to the inner codec.
   */
  def rangeCodec[A](inner: Codec[A], pgType: skunk.data.Type): Codec[Range[A]] =
    Codec.simple[Range[A]](
      encodeRange(_, inner),
      decodeRange(_, inner),
      pgType
    )

  private def encodeRange[A](r: Range[A], inner: Codec[A]): String =
    r match {
      case Range.Empty => "empty"
      case Range.Bounds(lower, upper, li, ui) =>
        val lb = if (li) "[" else "("
        val ub = if (ui) "]" else ")"
        val lv = lower.fold("")(a => quoteIfNeeded(encodeElem(a, inner)))
        val uv = upper.fold("")(a => quoteIfNeeded(encodeElem(a, inner)))
        s"$lb$lv,$uv$ub"
    }

  private def encodeElem[A](a: A, inner: Codec[A]): String =
    inner.encode(a).headOption.flatten.map(_.value).getOrElse("")

  private def quoteIfNeeded(s: String): String =
    if (s.exists(c => c == ' ' || c == ',' || c == '"' || c == '[' || c == ']' || c == '(' || c == ')' || c == '\\'))
      "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    else s

  private def decodeRange[A](s: String, inner: Codec[A]): Either[String, Range[A]] = {
    val t = s.trim
    if (t == "empty") Right(Range.Empty)
    else if (t.length < 3) Left(s"invalid range literal: $s")
    else {
      val lowerInclusive = t.head == '['
      val upperInclusive = t.last == ']'
      if ((!lowerInclusive && t.head != '(') || (!upperInclusive && t.last != ')'))
        Left(s"invalid range bounds: $s")
      else {
        val content = t.substring(1, t.length - 1)
        splitAtComma(content) match {
          case None => Left(s"invalid range content (no comma): $s")
          case Some((lowerStr, upperStr)) =>
            for {
              lower <- if (lowerStr.isEmpty) Right(None)
                       else decodeElem(unquote(lowerStr), inner).map(Some(_))
              upper <- if (upperStr.isEmpty) Right(None)
                       else decodeElem(unquote(upperStr), inner).map(Some(_))
            } yield Range.Bounds(lower, upper, lowerInclusive, upperInclusive)
        }
      }
    }
  }

  private def decodeElem[A](s: String, inner: Codec[A]): Either[String, A] =
    inner.decode(0, List(Some(s))).left.map(_.message)

  /** Split `lower,upper` at the comma that is not inside a quoted section. */
  private def splitAtComma(s: String): Option[(String, String)] = {
    var i       = 0
    var inQuote = false
    while (i < s.length) {
      val c = s(i)
      if (c == '"') {
        // handle escaped quote ("")
        if (inQuote && i + 1 < s.length && s(i + 1) == '"') i += 2
        else { inQuote = !inQuote; i += 1 }
      } else if (c == ',' && !inQuote) {
        return Some((s.substring(0, i), s.substring(i + 1)))
      } else i += 1
    }
    None
  }

  /** Strip outer double-quotes and unescape `\"` / `\\` inside. */
  private def unquote(s: String): String =
    if (s.startsWith("\"") && s.endsWith("\"")) {
      s.substring(1, s.length - 1)
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
    } else s

}
