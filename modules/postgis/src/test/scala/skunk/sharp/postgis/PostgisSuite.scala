package skunk.sharp.postgis

import skunk.postgis.{Coordinate, Geometry, Point, SRID}
import skunk.sharp.dsl.*
import skunk.sharp.pg.PgTypeFor

import java.util.UUID

object PostgisSuite {
  case class Site(id: UUID, geom: Point)
}

class PostgisSuite extends munit.FunSuite {
  import PostgisSuite.*

  test("postgis PgTypeFor[Geometry] carries requiredExtension = 'postgis'") {
    assertEquals(PgTypeFor[Geometry].requiredExtension, Some("postgis"))
  }

  test("postgis PgTypeFor[Point] carries requiredExtension = 'postgis'") {
    assertEquals(PgTypeFor[Point].requiredExtension, Some("postgis"))
  }

  test("a Point-typed column auto-flags the postgis extension on the relation") {
    val t = Table.of[Site]("sites")
    assertEquals(t.requiredExtensions, Set("postgis"))
  }

  test("explicit-codec column path auto-discovers postgis via PgTypes.extensionByType") {
    val t = Table.builder("sites")
      .column("id", skunk.codec.all.uuid)
      .column("geom", skunk.postgis.codecs.all.point)
      .build
    assertEquals(t.requiredExtensions, Set("postgis"))
  }

  test("ST_DWithin renders correct SQL") {
    val t     = Table.of[Site]("sites")
    val cols  = ColumnsView(t.columns)
    val probe = PgPostgis.setSRID(
      PgPostgis.makePoint(Param.bind(-6.26), Param.bind(53.34)),
      Param.bind(4326)
    )
    val w = cols.geom.dWithin(probe, Param.bind(1000.0))
    assertEquals(
      w.fragment.sql,
      """ST_DWithin("geom", ST_SetSRID(ST_MakePoint($1, $2), $3), $4)"""
    )
  }

  test("ST_Distance and ST_Within render correct SQL") {
    val t      = Table.of[Site]("sites")
    val cols   = ColumnsView(t.columns)
    val origin = Param.bind[Point](Point(Some(SRID(4326)), Coordinate.xy(0.0, 0.0)))
    assertEquals(PgPostgis.distance(cols.geom, origin).fragment.sql, """ST_Distance("geom", $1)""")
    val poly = Param.bind[Geometry](Point(Some(SRID(4326)), Coordinate.xy(0.0, 0.0)))
    assertEquals(cols.geom.within(poly).fragment.sql, """ST_Within("geom", $1)""")
  }

  test("bbox operators render their symbolic SQL") {
    val t      = Table.of[Site]("sites")
    val cols   = ColumnsView(t.columns)
    val origin = Param.bind[Geometry](Point(Some(SRID(4326)), Coordinate.xy(0.0, 0.0)))
    assertEquals(cols.geom.bboxOverlaps(origin).fragment.sql, """"geom" && $1""")
    assertEquals(cols.geom.bboxContains(origin).fragment.sql, """"geom" ~ $1""")
    assertEquals(cols.geom.bboxWithin(origin).fragment.sql, """"geom" @ $1""")
  }

  test("ST_X / ST_Y / ST_Area / ST_Length render correct SQL") {
    val t    = Table.of[Site]("sites")
    val cols = ColumnsView(t.columns)
    assertEquals(PgPostgis.x(cols.geom).fragment.sql, """ST_X("geom")""")
    assertEquals(PgPostgis.y(cols.geom).fragment.sql, """ST_Y("geom")""")
    assertEquals(PgPostgis.area(cols.geom).fragment.sql, """ST_Area("geom")""")
    assertEquals(PgPostgis.length(cols.geom).fragment.sql, """ST_Length("geom")""")
  }
}
