package skunk.sharp

/**
 * `skunk-sharp-postgis` — PostGIS support layered on top of [[skunk.postgis]].
 *
 * skunk-postgis owns the value types (`Geometry`, `Point`, `LineString`, `Polygon`, …) and the EWKB codecs; this module
 * adds the bits skunk-sharp users need:
 *
 *   - `PgTypeFor[Geometry]` and per-shape `PgTypeFor`s, each tagged with `requiredExtension = Some("postgis")` so the
 *     schema validator surfaces a missing extension as a [[skunk.sharp.validation.Mismatch.ExtensionMissing]].
 *   - A function bundle [[PgPostgis]] / `Pg` namespace covering the common `ST_*` calls (distance, predicates,
 *     constructors, accessors).
 *   - Operator extensions on `TypedExpr[Geometry, A]` — bounding-box `&&` and `~`, plus the same `ST_*` predicates
 *     spelled as English methods (`.distance`, `.dWithin`, `.contains`, `.within`, `.intersects`, …).
 *
 * Requires `CREATE EXTENSION postgis;` on the target database. The Postgres `geometry` type also auto-registers in
 * [[skunk.sharp.pg.PgTypes.extensionByType]] so the validator catches the dependency even on explicit-codec column
 * paths (`.column("loc", skunk.postgis.codecs.all.geometry)`).
 */
package object postgis extends PgTypeFors {

  /** Name of the Postgres extension this module requires — pass to `SchemaValidator` if you need it explicit. */
  val RequiredExtension: String = "postgis"

}
