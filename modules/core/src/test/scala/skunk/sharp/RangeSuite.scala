package skunk.sharp

import skunk.sharp.dsl.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate
import java.util.UUID

object RangeSuite {
  case class Booking(id: UUID, period: PgRange[LocalDate])
}

class RangeSuite extends munit.FunSuite {
  import RangeSuite.*

  private val bookings = Table.of[Booking]("bookings").withPrimary("id")

  // -------- Accessor functions ------------------------------------------------------

  test("rangeLower renders lower(col)") {
    val af = bookings.select(b => Pg.rangeLower(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT lower("period") FROM "bookings"""")
  }

  test("rangeUpper renders upper(col)") {
    val af = bookings.select(b => Pg.rangeUpper(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT upper("period") FROM "bookings"""")
  }

  test("rangeIsEmpty renders isempty(col)") {
    val af = bookings.select(b => Pg.rangeIsEmpty(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT isempty("period") FROM "bookings"""")
  }

  test("rangeLowerInc renders lower_inc(col)") {
    val af = bookings.select(b => Pg.rangeLowerInc(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT lower_inc("period") FROM "bookings"""")
  }

  test("rangeUpperInc renders upper_inc(col)") {
    val af = bookings.select(b => Pg.rangeUpperInc(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT upper_inc("period") FROM "bookings"""")
  }

  test("rangeLowerInf renders lower_inf(col)") {
    val af = bookings.select(b => Pg.rangeLowerInf(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT lower_inf("period") FROM "bookings"""")
  }

  test("rangeUpperInf renders upper_inf(col)") {
    val af = bookings.select(b => Pg.rangeUpperInf(b.period)).compile.af
    assertEquals(af.fragment.sql, """SELECT upper_inf("period") FROM "bookings"""")
  }

  // -------- Constructor functions ---------------------------------------------------

  test("int4range(lo, hi) renders int4range(lo, hi)") {
    val af = empty.select(_ => Pg.int4range(param(1), param(10))).compile.af
    assert(af.fragment.sql.contains("int4range("), af.fragment.sql)
  }

  test("daterange(lo, hi) renders daterange(lo, hi)") {
    val af = empty.select(_ => Pg.daterange(param(LocalDate.of(2024, 1, 1)), param(LocalDate.of(2024, 12, 31)))).compile.af
    assert(af.fragment.sql.contains("daterange("), af.fragment.sql)
  }

  test("int4range(lo, hi, bounds) passes bounds as param") {
    val af = empty.select(_ => Pg.int4range(param(1), param(10), "[]")).compile.af
    assert(af.fragment.sql.contains("int4range("), af.fragment.sql)
    assert(af.fragment.sql.contains("$3"), af.fragment.sql) // bounds is the 3rd param
  }

  // -------- Operators ---------------------------------------------------------------

  test("@> contains renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.contains(param(PgRange[LocalDate](lower = Some(LocalDate.of(2024, 1, 1)), upper = Some(LocalDate.of(2024, 12, 31)))))).compile
    assert(q.af.fragment.sql.contains(""""period" @>"""), q.af.fragment.sql)
  }

  test("<@ containedBy renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.containedBy(param(PgRange.empty[LocalDate]))).compile
    assert(q.af.fragment.sql.contains(""""period" <@"""), q.af.fragment.sql)
  }

  test("&& overlaps renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.overlaps(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" &&"""), q.af.fragment.sql)
  }

  test("<< strictlyLeft renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.strictlyLeft(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" <<"""), q.af.fragment.sql)
  }

  test(">> strictlyRight renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.strictlyRight(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" >>"""), q.af.fragment.sql)
  }

  test("&< doesNotExtendRight renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.doesNotExtendRight(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" &<"""), q.af.fragment.sql)
  }

  test("&> doesNotExtendLeft renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.doesNotExtendLeft(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" &>"""), q.af.fragment.sql)
  }

  test("-|- adjacent renders correctly") {
    val q = bookings.select(b => b.id).where(b => b.period.adjacent(param(PgRange[LocalDate]()))).compile
    assert(q.af.fragment.sql.contains(""""period" -|-"""), q.af.fragment.sql)
  }

  test("@> containsElem renders element containment") {
    val q = bookings.select(b => b.id).where(b => b.period.containsElem(param(LocalDate.of(2024, 6, 15)))).compile
    assert(q.af.fragment.sql.contains(""""period" @>"""), q.af.fragment.sql)
  }

  test("rangeUnion / rangeIntersect / rangeDiff render arithmetic ops") {
    val af = bookings.select(b => (
      b.period.rangeUnion(param(PgRange[LocalDate]())),
      b.period.rangeIntersect(param(PgRange[LocalDate]())),
      b.period.rangeDiff(param(PgRange[LocalDate]()))
    )).compile.af
    assert(af.fragment.sql.contains(""""period" +"""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""period" *"""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""period" -"""), af.fragment.sql)
  }

  // -------- Table.of[T] derives daterange codec for PgRange[LocalDate] column ------

  test("Table.of[Booking] compiles: PgRange[LocalDate] column gets daterange codec") {
    val af = bookings.select.compile.af
    assertEquals(af.fragment.sql, """SELECT "id", "period" FROM "bookings"""")
  }

}
