package skunk.sharp.sharedops

import skunk.sharp.TypedExpr
import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.ltree.LTree
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.fts.TsVector
import skunk.sharp.pg.tags.PgRange

import java.time.LocalDate

object SharedOperatorsSuite {

  case class Row(
    tags: Arr[Int],
    period: PgRange[LocalDate],
    props: Hstore,
    path: LTree,
    doc: TsVector,
    extra: Option[TsVector]
  )

  val rows = Table.of[Row]("rows")
}

/**
 * `@>` / `<@` / `&&` / `||` are one typeclass-dispatched set of operators, available from `skunk.sharp.dsl.*` alone.
 */
class SharedOperatorsSuite extends munit.FunSuite {
  import SharedOperatorsSuite.rows

  private val c = rows.columnsView

  test("containment across arrays, ranges (incl. an element), hstore and ltree") {
    assertEquals(c.tags.contains(c.tags).fragment.sql, """("tags" @> "tags")""")
    assertEquals(c.tags.containedBy(c.tags).fragment.sql, """("tags" <@ "tags")""")
    assertEquals(c.period.contains(c.period).fragment.sql, """("period" @> "period")""")
    assertEquals(c.period.contains(Param[LocalDate]).fragment.sql, """("period" @> $1)""")
    assertEquals(Param[LocalDate].containedBy(c.period).fragment.sql, """($1 <@ "period")""")
    assertEquals(c.props.contains(c.props).fragment.sql, """("props" @> "props")""")
    assertEquals(c.path.containedBy(c.path).fragment.sql, """("path" <@ "path")""")
  }

  test("overlap on arrays and ranges") {
    assertEquals(c.tags.overlaps(c.tags).fragment.sql, """("tags" && "tags")""")
    assertEquals(c.period.overlaps(c.period).fragment.sql, """("period" && "period")""")
  }

  test("concatenation: result type per instance, Option when either side is nullable") {
    val _: TypedExpr[Arr[Int], ?]         = c.tags.concat(c.tags)
    val _: TypedExpr[LTree, ?]            = c.path.concat(c.path)
    val _: TypedExpr[TsVector, ?]         = c.doc.concat(c.doc)
    val _: TypedExpr[Option[TsVector], ?] = c.doc.concat(c.extra)
    assertEquals(c.path.concat(c.path).fragment.sql, """("path" || "path")""")
  }

  test("operators without an instance don't compile") {
    assert(compileErrors(
      "SharedOperatorsSuite.rows.columnsView.path.overlaps(SharedOperatorsSuite.rows.columnsView.path)"
    ).nonEmpty)
    assert(compileErrors(
      "SharedOperatorsSuite.rows.columnsView.props.concat(SharedOperatorsSuite.rows.columnsView.props)"
    ).nonEmpty)
  }
}
