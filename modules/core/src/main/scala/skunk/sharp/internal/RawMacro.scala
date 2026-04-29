package skunk.sharp.internal

import skunk.AppliedFragment

import scala.quoted.*

/**
 * Macro implementation for `TypedExpr.raw`. When the input string is a compile-time constant — the common
 * case across the DSL: keywords, function names, operator symbols — splice in a call to
 * [[RawConstants.intern]], so every same-string call site resolves to a single shared `AppliedFragment`. When
 * the string is built at runtime (table aliases, dynamic operator symbols), splice in
 * [[RawConstants.rawDynamic]], which always allocates fresh.
 *
 * Net effect: the only places that allocate a fresh `AppliedFragment` per call are the ones whose SQL is
 * genuinely dynamic — `whereRaw` / `havingRaw` payloads, runtime-built alias / cast / type-name strings.
 * Every static piece of structure interns once at first compile and is reused forever.
 */
private[sharp] object RawMacro {

  def impl(sql: Expr[String])(using Quotes): Expr[AppliedFragment] =
    sql.value match {
      case Some(_) => '{ RawConstants.intern($sql) }
      case None    => '{ RawConstants.rawDynamic($sql) }
    }

}
