# PostGIS — `skunk-sharp-postgis`

Spatial types and `ST_*` operators, layered on top of [skunk-postgis](https://github.com/typelevel/skunk/tree/main/modules/postgis).
**Shipped as a separate artifact** — skunk-postgis pulls in a scodec / EWKB dependency
that not every consumer wants, so the bridge module follows the same separation:

```scala
libraryDependencies += "com.ruiandrebatista" %% "skunk-sharp-postgis" % "@VERSION@"
```

Requires `CREATE EXTENSION postgis;` on the target database. The schema validator picks
this up automatically from any column with a geometry-typed `PgTypeFor`, and also from
the explicit-codec construction path — `Table.builder.column("loc", skunk.postgis.codecs.all.point)`
auto-flags the dependency too because `geometry` is registered in
`PgTypes.extensionByType`.

The Scala-side value types (`Geometry`, `Point`, `LineString`, `Polygon`, …) all come
from skunk-postgis; this module just wires them into the skunk-sharp DSL:

| Surface | Contents |
| --- | --- |
| `given PgTypeFor[T]` | one per concrete shape: `Geometry`, `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection` |
| Functions (`PgPostgis`) | `distance`, `dWithin`, `contains`, `within`, `intersects`, `crosses`, `overlaps`, `disjoint`, `stEquals`, `makePoint` (2-D / 3-D), `setSRID`, `transform`, `x`, `y`, `srid`, `area`, `length`, `perimeter`, `centroid`, `envelope`, `asText`, `asGeoJSON`, `geomFromText`, `geomFromGeoJSON` |
| Operators (extension methods on `TypedExpr[T <: Geometry, A]`) | `bboxOverlaps` (`&&`), `bboxContains` (`~`), `bboxWithin` (`@`), plus English-named ST-predicate aliases: `.distance`, `.dWithin`, `.contains`, `.within`, `.intersects`, `.crosses`, `.overlapsSpatial`, `.disjoint`, `.stEquals` |

Every geometry-typed slot is generic over `T <: Geometry`, so a column declared as
`Point` (or `Polygon`, …) slots into an `ST_*` call without explicit widening.

## A worked example

```scala mdoc:silent
import skunk.postgis.Point
import skunk.sharp.dsl.*
import skunk.sharp.postgis.*
import skunk.sharp.postgis.given

import java.util.UUID

case class Building(id: UUID, name: String, geom: Point)
val buildings = Table.of[Building]("buildings").withPrimary("id").withDefault("id")

// `Table.of[Building]` auto-detects PostGIS via `PgTypeFor[Point]`:
//   buildings.requiredExtensions == Set("postgis")
```

Filtering buildings within a radius of a given lat/lon is the canonical PostGIS query —
build the probe point with `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` and feed it to
`ST_DWithin`:

```scala mdoc:silent
val lat    = Param[Double]
val lon    = Param[Double]
val radius = Param[Double]

val nearby = buildings.select
  .where { b =>
    val probe = PgPostgis.setSRID(
      PgPostgis.makePoint(lon, lat),
      lit(4326)
    )
    b.geom.dWithin(probe, radius)
  }
  .compile  // CompiledQuery[(Double, Double, Double), NamedRow]
```

`b.geom` is a `TypedColumn[Point, false, "geom"]`; `b.geom.dWithin(probe, radius)`
widens to `Geometry` internally so it threads through `ST_DWithin` cleanly. The
captured-args tuple is exactly the three `Param`s in the order they appear.

## Bounding-box vs. exact predicates

The cheap `&&` / `~` / `@` bounding-box operators short-circuit on a GiST index and
return `true` for anything whose bbox overlaps / contains / is-contained-by — they're a
*subset* of the exact predicates, not a replacement. The conventional PostGIS pattern is
`bbox AND exact` so the index does the heavy lifting and the exact predicate filters
the survivors:

```scala mdoc:silent
val area = Param[skunk.postgis.Geometry]

val within = buildings.select
  .where(b => b.geom.bboxOverlaps(area) && b.geom.within(area))
  .compile
```

## Geographies and SRIDs

The example uses `geometry(Point, 4326)`. For distance queries that need real-world
metres, `geography` is the right column type (PostGIS does spherical math). skunk-postgis
currently bundles codecs only for `geometry`; if you store a `geography` column, use the
explicit-codec construction path:

```scala
import skunk.codec.all as pg
val table = Table.builder("places")
  .column("id", pg.uuid)
  .column("loc", skunk.postgis.codecs.all.geometry)   // wire-compatible with geography
  .build
```

…and write the casts in your queries (`geom::geography`). A first-class `geography`
codec is the natural next step.
