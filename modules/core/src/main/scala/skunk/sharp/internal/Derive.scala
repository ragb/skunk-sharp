package skunk.sharp.internal

import skunk.sharp.Column
import skunk.sharp.pg.{PgTypeFor, PgTypes}

import scala.compiletime.{constValue, erasedValue, summonInline}

/**
 * Map a `Mirror.ProductOf[T]`'s label/type tuples to the type-level tuple of [[skunk.sharp.Column]]s.
 *
 * Option-wrapped fields become nullable columns; everything else is non-null. Nothing about defaults is inferred from
 * the case class — that's declared explicitly via `withDefault("name")` on the resulting table.
 */
type ColumnsFromMirror[Labels <: Tuple, Types <: Tuple] <: Tuple = (Labels, Types) match {
  case (EmptyTuple, EmptyTuple)   => EmptyTuple
  case (l *: lt, Option[t] *: tt) =>
    Column[Option[t], l & String & Singleton, true, false] *: ColumnsFromMirror[lt, tt]
  case (l *: lt, t *: tt) =>
    Column[t, l & String & Singleton, false, false] *: ColumnsFromMirror[lt, tt]
}

/**
 * Runtime counterpart to [[ColumnsFromMirror]]: builds the tuple of [[Column]] values by summoning a [[PgTypeFor]] for
 * each field type. The column's skunk `Type` is read from the codec (`codec.types.head`) so no separate type registry
 * is needed.
 */
inline def deriveColumns[Labels <: Tuple, Types <: Tuple]: Tuple =
  inline erasedValue[Labels] match {
    case _: EmptyTuple =>
      EmptyTuple
    case _: (l *: ls) =>
      inline erasedValue[Types] match {
        case _: (Option[t] *: ts) =>
          val pf    = summonInline[PgTypeFor[t]]
          val name  = constValue[l].asInstanceOf[String & Singleton]
          val codec = pf.codec.opt
          Column(
            name = name,
            tpe = PgTypes.typeOf(pf.codec),
            codec = codec,
            isNullable = true,
            hasDefault = false
          ) *: deriveColumns[ls, ts]
        case _: (t *: ts) =>
          val pf   = summonInline[PgTypeFor[t]]
          val name = constValue[l].asInstanceOf[String & Singleton]
          Column(
            name = name,
            tpe = PgTypes.typeOf(pf.codec),
            codec = pf.codec,
            isNullable = false,
            hasDefault = false
          ) *: deriveColumns[ls, ts]
      }
  }
