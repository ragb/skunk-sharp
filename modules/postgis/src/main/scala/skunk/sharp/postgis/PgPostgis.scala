package skunk.sharp.postgis

import skunk.postgis.*
import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.where.Where

/**
 * PostGIS `ST_*` function helpers — the conventional surface every PostGIS app reaches for. Mix into your own `Pg`
 * bundle, or call via the [[PgPostgis]] namespace. Every argument is a typed expression so `Args` threads naturally
 * through whatever you compose.
 *
 * Every geometry-input slot is generic over `T <: Geometry` (and pair-slots over two such Ts) so columns declared as
 * the specific subtypes — `TypedColumn[Point, …]`, `TypedColumn[Polygon, …]`, … — slot in without explicit widening.
 * Internally we [[widen]] to the `Geometry` super-type because skunk-postgis's per-shape codec is just the base
 * `geometry` codec under a runtime cast, so the operation is safe.
 *
 * Scope: distance / predicates / constructors / accessors / format conversion. Heavier surface (raster, topology,
 * sfcgal) is deliberately out of scope — those have their own extensions and are easy to add downstream by following
 * the same shape.
 */
trait PgPostgis {

  /**
   * Widen a `TypedExpr[T <: Geometry, A]` to `TypedExpr[Geometry, A]` — a runtime no-op. The cast is safe because every
   * `T <: Geometry` IS a `Geometry`; only `Codec[T]`'s invariance forces the explicit assertion.
   */
  private[postgis] inline def widen[T <: Geometry, A](e: TypedExpr[T, A]): TypedExpr[Geometry, A] =
    e.asInstanceOf[TypedExpr[Geometry, A]]

  // -------- Distance & nearest-neighbour --------------------------------------------------------

  /** `ST_Distance(a, b)` — Cartesian distance in the geometry's SRID units (meters for geography). */
  inline def distance[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Double, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Double, A, B]("ST_Distance")(widen(a), widen(b))

  /** `ST_DWithin(a, b, radius)` — `true` iff `a` and `b` are within `radius`. */
  inline def dWithin[T1 <: Geometry, T2 <: Geometry, A, B, C](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B],
    radius: TypedExpr[Double, C]
  ): TypedExpr[Boolean, Where.Concat[A, Where.Concat[B, C]]] = {
    val aG    = widen(a)
    val bG    = widen(b)
    val mid   = TypedExpr.combineSepInl[B, C](bG.fragment, ", ", radius.fragment)
    val inner = TypedExpr.combineSepInl[A, Where.Concat[B, C]](aG.fragment, ", ", mid)
    val frag  = TypedExpr.wrap("ST_DWithin(", inner, ")")
    TypedExpr[Boolean, Where.Concat[A, Where.Concat[B, C]]](frag, skunk.codec.all.bool)
  }

  // -------- Topological predicates --------------------------------------------------------------

  inline def contains[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Contains")(widen(a), widen(b))

  inline def within[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Within")(widen(a), widen(b))

  inline def intersects[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Intersects")(widen(a), widen(b))

  inline def crosses[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Crosses")(widen(a), widen(b))

  inline def overlaps[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Overlaps")(widen(a), widen(b))

  inline def disjoint[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Disjoint")(widen(a), widen(b))

  inline def stEquals[T1 <: Geometry, T2 <: Geometry, A, B](
    a: TypedExpr[T1, A],
    b: TypedExpr[T2, B]
  ): TypedExpr[Boolean, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Geometry, Boolean, A, B]("ST_Equals")(widen(a), widen(b))

  // -------- Construction ------------------------------------------------------------------------

  /** `ST_MakePoint(x, y)` — a 2-D point with no SRID (set one via [[setSRID]] if needed). */
  inline def makePoint[A, B](
    x: TypedExpr[Double, A],
    y: TypedExpr[Double, B]
  ): TypedExpr[Geometry, Where.Concat[A, B]] =
    PgFunction.binary[Double, Double, Geometry, A, B]("ST_MakePoint")(x, y)

  /** `ST_MakePoint(x, y, z)` — 3-D variant. */
  inline def makePoint[A, B, C](
    x: TypedExpr[Double, A],
    y: TypedExpr[Double, B],
    z: TypedExpr[Double, C]
  ): TypedExpr[Geometry, Where.Concat[A, Where.Concat[B, C]]] = {
    val mid   = TypedExpr.combineSepInl[B, C](y.fragment, ", ", z.fragment)
    val inner = TypedExpr.combineSepInl[A, Where.Concat[B, C]](x.fragment, ", ", mid)
    val frag  = TypedExpr.wrap("ST_MakePoint(", inner, ")")
    TypedExpr[Geometry, Where.Concat[A, Where.Concat[B, C]]](frag, skunk.postgis.codecs.all.geometry)
  }

  /** `ST_SetSRID(geom, srid)` — tag a geometry with an SRID. */
  inline def setSRID[T <: Geometry, A, B](
    geom: TypedExpr[T, A],
    srid: TypedExpr[Int, B]
  ): TypedExpr[Geometry, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Int, Geometry, A, B]("ST_SetSRID")(widen(geom), srid)

  /** `ST_Transform(geom, toSrid)` — reproject to a different SRID. */
  inline def transform[T <: Geometry, A, B](
    geom: TypedExpr[T, A],
    toSrid: TypedExpr[Int, B]
  ): TypedExpr[Geometry, Where.Concat[A, B]] =
    PgFunction.binary[Geometry, Int, Geometry, A, B]("ST_Transform")(widen(geom), toSrid)

  // -------- Accessors ---------------------------------------------------------------------------

  /** `ST_X(geom)` — X coordinate (longitude in geographic coords). Defined only for Point inputs. */
  inline def x[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Double, A] =
    PgFunction.unary[Geometry, Double, A]("ST_X")(widen(geom))

  /** `ST_Y(geom)` — Y coordinate (latitude in geographic coords). */
  inline def y[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Double, A] =
    PgFunction.unary[Geometry, Double, A]("ST_Y")(widen(geom))

  /** `ST_SRID(geom)` — read the assigned SRID. */
  inline def srid[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Int, A] =
    PgFunction.unary[Geometry, Int, A]("ST_SRID")(widen(geom))

  /** `ST_Area(geom)` — area in the geometry's SRID units. */
  inline def area[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Double, A] =
    PgFunction.unary[Geometry, Double, A]("ST_Area")(widen(geom))

  /** `ST_Length(geom)` — length of a (multi)linestring; 0 for areal geometries. */
  inline def length[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Double, A] =
    PgFunction.unary[Geometry, Double, A]("ST_Length")(widen(geom))

  /** `ST_Perimeter(geom)` — perimeter of a (multi)polygon. */
  inline def perimeter[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Double, A] =
    PgFunction.unary[Geometry, Double, A]("ST_Perimeter")(widen(geom))

  /** `ST_Centroid(geom)`. */
  inline def centroid[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Geometry, A] =
    PgFunction.unary[Geometry, Geometry, A]("ST_Centroid")(widen(geom))

  /** `ST_Envelope(geom)` — minimum bounding rectangle as a Polygon. */
  inline def envelope[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[Geometry, A] =
    PgFunction.unary[Geometry, Geometry, A]("ST_Envelope")(widen(geom))

  // -------- Format conversion -------------------------------------------------------------------

  /** `ST_AsText(geom)` — WKT representation. */
  inline def asText[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[String, A] =
    PgFunction.unary[Geometry, String, A]("ST_AsText")(widen(geom))

  /** `ST_AsGeoJSON(geom)` — GeoJSON representation. */
  inline def asGeoJSON[T <: Geometry, A](geom: TypedExpr[T, A]): TypedExpr[String, A] =
    PgFunction.unary[Geometry, String, A]("ST_AsGeoJSON")(widen(geom))

  /** `ST_GeomFromText(wkt)`. */
  inline def geomFromText[A](wkt: TypedExpr[String, A]): TypedExpr[Geometry, A] =
    PgFunction.unary[String, Geometry, A]("ST_GeomFromText")(wkt)

  /** `ST_GeomFromText(wkt, srid)`. */
  inline def geomFromText[A, B](
    wkt: TypedExpr[String, A],
    srid: TypedExpr[Int, B]
  ): TypedExpr[Geometry, Where.Concat[A, B]] =
    PgFunction.binary[String, Int, Geometry, A, B]("ST_GeomFromText")(wkt, srid)

  /** `ST_GeomFromGeoJSON(json)`. */
  inline def geomFromGeoJSON[A](json: TypedExpr[String, A]): TypedExpr[Geometry, A] =
    PgFunction.unary[String, Geometry, A]("ST_GeomFromGeoJSON")(json)

}

object PgPostgis extends PgPostgis
