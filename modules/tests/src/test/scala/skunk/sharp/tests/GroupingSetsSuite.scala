package skunk.sharp.tests

import cats.data.NonEmptyList
import skunk.sharp.dsl.*

object GroupingSetsSuite {
  case class Sale(id: Int, year: Int, quarter: Int, amount: Int)
}

class GroupingSetsSuite extends PgFixture {
  import GroupingSetsSuite.Sale

  private val sales = Table.of[Sale]("sales").withPrimary("id")

  private def seedSales(s: skunk.Session[cats.effect.IO], idBase: Int) =
    sales.insert.values(
      NonEmptyList.of(
        (id = idBase, year = 2024, quarter = 1, amount = 100),
        (id = idBase + 1, year = 2024, quarter = 2, amount = 200),
        (id = idBase + 2, year = 2025, quarter = 1, amount = 300),
        (id = idBase + 3, year = 2025, quarter = 2, amount = 400)
      )
    ).compile.run(s)

  // ROLLUP (year, quarter) produces 4 leaf rows + 2 year-subtotals + 1 grand total = 7 rows
  test("ROLLUP (year, quarter) produces leaf + subtotal + grand-total rows") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _    <- seedSales(s, idBase = 100)
          rows <- sales.select
            .where(ss => ss.id >= 100 && ss.id <= 103)
            .groupBy(ss => Pg.rollup(ss.year, ss.quarter))
            .select(ss => (Pg.grouping(ss.year, ss.quarter), Pg.countAll))
            .compile
            .run(s)
          // grouping(year, quarter): 0 = leaf, 1 = year-subtotal, 3 = grand total
          bitmasks = rows.map(_._1).toSet
          _        = assert(bitmasks.contains(0), s"missing leaf rows (bitmask 0), got $bitmasks")
          _        = assert(bitmasks.contains(1), s"missing quarter-subtotal rows (bitmask 1), got $bitmasks")
          _        = assert(bitmasks.contains(3), s"missing grand-total row (bitmask 3), got $bitmasks")
          _        = assertEquals(rows.size, 7)
        } yield ()
      }
    }
  }

  test("CUBE (year, quarter) produces all grouping combinations") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _    <- seedSales(s, idBase = 200)
          rows <- sales.select
            .where(ss => ss.id >= 200 && ss.id <= 203)
            .groupBy(ss => Pg.cube(ss.year, ss.quarter))
            .select(ss => (Pg.grouping(ss.year, ss.quarter), Pg.countAll))
            .compile
            .run(s)
          bitmasks = rows.map(_._1).toSet
          _        = assert(bitmasks.contains(0), s"missing (year, quarter) rows, got $bitmasks")
          _        = assert(bitmasks.contains(1), s"missing (year) rows, got $bitmasks")
          _        = assert(bitmasks.contains(2), s"missing (quarter) rows, got $bitmasks")
          _        = assert(bitmasks.contains(3), s"missing grand-total row, got $bitmasks")
          // CUBE (year, quarter) = GROUPING SETS ((y,q),(y),(q),()) = 4+2+2+1 = 9 distinct rows
          _ = assertEquals(rows.size, 9)
        } yield ()
      }
    }
  }

  test("GROUPING SETS ((year, quarter), (year), ()) produces explicit sets") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _    <- seedSales(s, idBase = 300)
          rows <- sales.select
            .where(ss => ss.id >= 300 && ss.id <= 303)
            .groupBy(ss =>
              Pg.groupingSets(
                Seq(ss.year, ss.quarter),
                Seq(ss.year),
                Pg.emptyGroup
              )
            )
            .select(ss => (Pg.grouping(ss.year, ss.quarter), Pg.countAll))
            .compile
            .run(s)
          bitmasks = rows.map(_._1).toSet
          _        = assert(bitmasks.contains(0), s"missing (year,quarter) rows, got $bitmasks")
          _        = assert(bitmasks.contains(1), s"missing (year) rows, got $bitmasks")
          _        = assert(bitmasks.contains(3), s"missing grand-total rows, got $bitmasks")
          // 4 leaf + 2 year-subtotal + 1 grand total = 7
          _ = assertEquals(rows.size, 7)
        } yield ()
      }
    }
  }

  test("GROUPING() function returns correct bitmask for grand-total row") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _    <- seedSales(s, idBase = 400)
          rows <- sales.select
            .where(ss => ss.id >= 400 && ss.id <= 403)
            .groupBy(ss => Pg.rollup(ss.year))
            .select(ss => (Pg.grouping(ss.year), Pg.countAll))
            .compile
            .run(s)
          // bitmask 0 = year is present; bitmask 1 = year is aggregated (grand total)
          grandTotal = rows.find(_._1 == 1)
          _          = assert(grandTotal.isDefined, s"no grand-total row found, rows=$rows")
          _          = assertEquals(grandTotal.get._2, 4L)
        } yield ()
      }
    }
  }
}
