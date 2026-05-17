package skunk.sharp.example.repository

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import skunk.postgis.Point
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.example.domain.BuildingRow
import skunk.sharp.postgis.PgPostgis
import skunk.sharp.postgis.given

import java.util.UUID

trait BuildingRepository {
  def findFiltered(filters: List[BuildingFilter]): Kleisli[Stream[IO, *], Session[IO], BuildingRow]
  def findById(id: UUID): Kleisli[IO, Session[IO], Option[BuildingRow]]
  def create(data: BuildingRow.Create): Kleisli[IO, Session[IO], UUID]
  def patch(id: UUID, data: BuildingRow.Patch): Kleisli[IO, Session[IO], Option[BuildingRow]]
  def delete(id: UUID): Kleisli[IO, Session[IO], Unit]
}

object BuildingRepository {

  val live: BuildingRepository = new BuildingRepository {
    private val t  = BuildingRow.table
    private val cv = t.columnsView

    private val selectRow =
      t.select(b => (b.id, b.name, b.address, b.geom)).to[BuildingRow]

    private val findAllQ  = selectRow.compile
    private val findByIdQ = selectRow.where(b => b.id === Param[UUID]).compile

    // Args = (String, String, Point) — matches Create's field order.
    private val createQ =
      t.insert
        .withParams((name = Param[String], address = Param[String], geom = Param[Point]))
        .returning(b => b.id)
        .compile

    private val deleteQ =
      t.delete.where(b => b.id === Param[UUID]).compile

    /**
     * Translate one filter to a `Where[Void]`. `WithinMetersOf` materialises as
     * `ST_DWithin(geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326), meters)` — same shape PostGIS apps reach for
     * everywhere. The intermediate `Param.bind`ed pieces keep the surface `Where[Void]` so the AND-fold composes
     * uniformly with the other filter cases.
     */
    private def toWhere(f: BuildingFilter): Where[skunk.Void] = f match {
      case BuildingFilter.NameContains(s)                  => cv.name.ilike(Param.bind(s"%$s%"))
      case BuildingFilter.IdsIn(ids)                       => cv.id.in(ids.map(Param.bind(_)))
      case BuildingFilter.WithinMetersOf(lat, lon, meters) =>
        val probe = PgPostgis.setSRID(
          PgPostgis.makePoint(Param.bind(lon), Param.bind(lat)),
          Param.bind(4326)
        )
        Where(PgPostgis.dWithin(cv.geom, probe, Param.bind(meters)).fragment)
    }

    def findFiltered(filters: List[BuildingFilter]): Kleisli[Stream[IO, *], Session[IO], BuildingRow] =
      if filters.isEmpty then findAllQ.streamKF[IO]()
      else selectRow.where(_ => allOf(filters.map(toWhere)*)).compile.streamKF[IO]()

    def findById(id: UUID): Kleisli[IO, Session[IO], Option[BuildingRow]] =
      findByIdQ.optionK[IO](id)

    def create(data: BuildingRow.Create): Kleisli[IO, Session[IO], UUID] =
      createQ.uniqueK[IO]((data.name, data.address, data.geom))

    def patch(id: UUID, data: BuildingRow.Patch): Kleisli[IO, Session[IO], Option[BuildingRow]] =
      if (data.name.isEmpty && data.address.isEmpty && data.geom.isEmpty) findById(id)
      else
        t.update
          .patch(data)
          .where(b => b.id === Param.bind(id))
          .returningAll.to[BuildingRow]
          .compile.optionK[IO]

    def delete(id: UUID): Kleisli[IO, Session[IO], Unit] =
      deleteQ.runK[IO](id).void
  }

}
