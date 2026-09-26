package skunk.sharp.iron

import io.github.iltotore.iron.Constraint

import scala.compiletime.constValue
import scala.compiletime.ops.any.ToString
import scala.compiletime.ops.string.+

/**
 * Iron constraint for Postgres `numeric(P, S)`: at most `P - S` digits before the decimal point and at most `S` after
 * it. `BigDecimal :| Precision[10, 2]` maps to the `numeric(10, 2)` codec (like `String :| MaxLength[N]` → `varchar`).
 *
 * `BigDecimal` values aren't compile-time constants, so refine at runtime: `v.refineEither[Precision[10, 2]]`. Extra
 * scale is **rejected** rather than rounded (Postgres would round silently).
 */
final class Precision[P <: Int, S <: Int]

object Precision {

  /** Does `v` fit `numeric(p, s)`? Trailing zeros don't count; a value below 1 has no integer digits. */
  def fits(v: BigDecimal, p: Int, s: Int): Boolean = {
    val u         = v.bigDecimal.stripTrailingZeros
    val scale     = math.max(u.scale, 0)
    val intDigits = if (u.abs.compareTo(java.math.BigDecimal.ONE) < 0) 0 else u.precision - u.scale
    scale <= s && intDigits <= p - s
  }

  final class PrecisionConstraint[P <: Int, S <: Int] extends Constraint[BigDecimal, Precision[P, S]] {
    override inline def test(inline value: BigDecimal): Boolean = fits(value, constValue[P], constValue[S])

    override inline def message: String =
      constValue["Should fit numeric(" + ToString[P] + ", " + ToString[S] + ")"]

  }

  inline given [P <: Int, S <: Int]: PrecisionConstraint[P, S] = new PrecisionConstraint[P, S]

}
