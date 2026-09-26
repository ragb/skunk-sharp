package skunk.sharp.contrib.pgvector

import scala.quoted.{Expr, Quotes, Type, Varargs}

/** `PgVector(0.1f, 0.2f, 0.3f)`: counts the written-out values at compile time and types the result `PgVector[3]`. */
object PgVectorMacro {

  def literal(values: Expr[Seq[Float]])(using q: Quotes): Expr[PgVector[? <: Int]] = {
    import q.reflect.*
    values match {
      case Varargs(elems) =>
        if (elems.isEmpty) report.errorAndAbort("skunk-sharp: a PgVector needs at least one value")
        ConstantType(IntConstant(elems.size)).asType match {
          case '[n] =>
            '{ PgVector.ofExactly[n & Int](IArray(${ Varargs(elems) }*)) }
        }
      case _ =>
        report.errorAndAbort(
          "skunk-sharp: PgVector(…) needs the values written out so their count is known at compile time — " +
            "for a runtime collection use PgVector.from[N](values) (Either) or PgVector.unsafeFrom[N](values)"
        )
    }
  }

}
