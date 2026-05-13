package skunk.sharp.postgis

import skunk.postgis.codecs.all as pgis
import skunk.postgis.*
import skunk.sharp.pg.PgTypeFor

/**
 * `PgTypeFor` instances for every concrete geometry type skunk-postgis ships. Each is built on top of skunk's bundled
 * codec (EWKB wire format), tagged with `requiredExtension = Some("postgis")` so the schema validator picks up the
 * dependency automatically when a relation has any geometry-typed column.
 *
 * Mixed into the package object so a single `import skunk.sharp.postgis.*` brings everything (givens included) into
 * scope.
 */
trait PgTypeFors {

  given PgTypeFor[Geometry]           = PgTypeFor.instanceWithExtension(pgis.geometry, RequiredExtension)
  given PgTypeFor[Point]              = PgTypeFor.instanceWithExtension(pgis.point, RequiredExtension)
  given PgTypeFor[LineString]         = PgTypeFor.instanceWithExtension(pgis.lineString, RequiredExtension)
  given PgTypeFor[Polygon]            = PgTypeFor.instanceWithExtension(pgis.polygon, RequiredExtension)
  given PgTypeFor[MultiPoint]         = PgTypeFor.instanceWithExtension(pgis.multiPoint, RequiredExtension)
  given PgTypeFor[MultiLineString]    = PgTypeFor.instanceWithExtension(pgis.multiLineString, RequiredExtension)
  given PgTypeFor[MultiPolygon]       = PgTypeFor.instanceWithExtension(pgis.multiPolygon, RequiredExtension)
  given PgTypeFor[GeometryCollection] = PgTypeFor.instanceWithExtension(pgis.geometryCollection, RequiredExtension)

}
