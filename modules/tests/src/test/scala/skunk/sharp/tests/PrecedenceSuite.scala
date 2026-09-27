package skunk.sharp.tests

import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.data.Range

import java.time.LocalDate

/** Nested operators and multi-input nullability, evaluated by Postgres (#121, #122). */
class PrecedenceSuite extends PgFixture {

  private def day(d: Int) = LocalDate.of(2024, 1, d)

  test("nested range operators group as written: a - (b + c), not (a - b) + c") {
    withContainers { containers =>
      session(containers).use { s =>
        val q = empty
          .select(_ =>
            Param.named["a", PgRange[LocalDate]].rangeDiff(
              Param.named["b", PgRange[LocalDate]].rangeUnion(Param.named["c", PgRange[LocalDate]])
            )
          )
          .compile
        val a = PgRange[LocalDate](lower = Some(day(1)), upper = Some(day(10)))
        val b = PgRange[LocalDate](lower = Some(day(1)), upper = Some(day(3)))
        val c = PgRange[LocalDate](lower = Some(day(3)), upper = Some(day(5)))
        // a - (b + c) = [01-05, 01-10); the unparenthesised `a - b + c` would give [01-03, 01-10).
        q.unique(s)((a = a, b = b, c = c)).map { r =>
          assertEquals(r.asInstanceOf[Range.Bounds[LocalDate]].lower, Some(day(5)))
        }
      }
    }
  }

  test("a NULL second argument makes a two-input function's result NULL, and it decodes") {
    withContainers { containers =>
      session(containers).use { s =>
        val q = empty
          .select(_ => (Pg.power(lit(2.0), Param[Option[Double]]), Pg.atan2(lit(1.0), Param[Option[Double]])))
          .compile
        val _: QueryTemplate[(Option[Double], Option[Double]), (Option[Double], Option[Double])] = q
        assertIO(q.unique(s)((None, Some(1.0))), (None, Some(math.atan2(1.0, 1.0))))
      }
    }
  }
}
