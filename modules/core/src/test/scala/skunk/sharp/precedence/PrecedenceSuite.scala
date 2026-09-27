package skunk.sharp.precedence

import skunk.sharp.TypedExpr
import skunk.sharp.contrib.hstore.*
import skunk.sharp.contrib.pgtrgm.*
import skunk.sharp.contrib.pgvector.*
import skunk.sharp.dsl.*
import skunk.sharp.fts.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate

object PrecedenceSuite {

  case class Row(
    id: Int,
    embedding: PgVector[3],
    period: PgRange[LocalDate],
    props: Hstore,
    note: Option[String],
    tsv: Option[TsVector],
    x: Double,
    y: Option[Double]
  )

  val rows = Table.of[Row]("rows")
}

/** Infix operators render parenthesised (#121); multi-input functions lift nullability from every input (#122). */
class PrecedenceSuite extends munit.FunSuite {
  import PrecedenceSuite.rows

  private val c = rows.columnsView

  test("an operator nested in arithmetic keeps its grouping") {
    assertEquals(
      (c.embedding.cosineDistance(Param[PgVector[3]]) * 2.0).fragment.sql,
      """(("embedding" <=> $1) * 2.0::float8)"""
    )
  }

  test("nested range operators render grouped as written") {
    assertEquals(
      c.period.rangeDiff(c.period.rangeUnion(c.period)).fragment.sql,
      """("period" - ("period" + "period"))"""
    )
  }

  test("an operator result used as another operator's operand stays grouped") {
    assertEquals(c.props.deleteKey(lit("a")).hasKey(lit("b")).fragment.sql, """(("props" - 'a') ? 'b')""")
  }

  test("cast parenthesises a compound operand, not an atomic one") {
    assertEquals(
      c.embedding.cosineDistance(c.embedding).cast[Int].fragment.sql,
      // Redundant but always correct: whether a fragment is *wholly* wrapped can't be read off its parts safely.
      """(("embedding" <=> "embedding"))::int4"""
    )
    assertEquals(c.x.cast[Int].fragment.sql, """"x"::int4""")
  }

  test("two-input functions are Option when either input is nullable") {
    val _: TypedExpr[Option[Double], ?] = Pg.power(c.x, c.y)
    val _: TypedExpr[Option[Double], ?] = Pg.atan2(c.y, c.x)
    val _: TypedExpr[Double, ?]         = Pg.power(c.x, c.x)
    val _: TypedExpr[Option[Float], ?]  = c.note.trgmDistance(lit("x"))
    val _: TypedExpr[Option[Float], ?]  = Fts.tsRank(c.tsv, Fts.toTsQuery(lit("a")))
  }
}
