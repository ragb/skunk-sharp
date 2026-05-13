package skunk.sharp.example.api

import cats.effect.IO
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend
import sttp.model.StatusCode

import java.util.UUID

/**
 * End-to-end test for the buildings resource at `GET /api/v1/buildings`. Covers the PostGIS-backed `WithinMetersOf`
 * filter (`ST_DWithin`) plus the simpler name/id-set filters.
 *
 * Distance assertions use real-world coordinates: Dublin city centre, Trinity College, and Cork city — far enough
 * apart that the radius math is unambiguous even with the Cartesian-on-SRID-4326 caveat in the BuildingRepository
 * docs.
 */
class BuildingsFilterEndpointSuite extends ExampleAppFixture {

  private val createReq  = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.create, Some(baseUri))
  private val listReq    = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.list, Some(baseUri))
  private val getByIdReq = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.getById, Some(baseUri))

  // Approximate WGS84 lat/lon for a few places we can reason about.
  private val DublinCentre  = LatLon(53.3498, -6.2603)
  private val TrinityCollege = LatLon(53.3438, -6.2546)
  private val CorkCity      = LatLon(51.8985, -8.4756)

  private def createBuilding(name: String, l: LatLon)(using SttpBackend[IO, Fs2Streams[IO]]): IO[BuildingResponse] =
    createReq(CreateBuildingRequest(name, address = s"$name address", location = l)).sendOk

  private def listBuildings(q: BuildingFilterQuery)(using SttpBackend[IO, Fs2Streams[IO]]): IO[List[BuildingResponse]] =
    listReq(q).sendOk

  test("create + getById round-trips a building including its lat/lon location") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          createBuilding("HQ", DublinCentre).flatMap { b =>
            assertEquals(b.location, DublinCentre)
            getByIdReq(b.id).sendOk.map { back =>
              assertEquals(back.name, "HQ")
              // Tiny tolerance for the floating-point round-trip through PostGIS.
              assert(math.abs(back.location.lat - DublinCentre.lat) < 1e-9, "lat round-trip")
              assert(math.abs(back.location.lon - DublinCentre.lon) < 1e-9, "lon round-trip")
            }
          }
      }
    }
  }

  test("WithinMetersOf (ST_DWithin) matches close buildings and rejects far-away ones") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _ <- createBuilding("HQ", DublinCentre)
            _ <- createBuilding("Lab", TrinityCollege)
            _ <- createBuilding("Cork-Office", CorkCity)
            // 2 km around Dublin Centre — should include HQ and Trinity (a few hundred metres away),
            // exclude Cork (~220 km).
            //
            // Note: the column is SRID-4326 `geometry`, so ST_DWithin's units are degrees. To compare
            // against real-world metres we'd want a `geography` column or an `ST_Transform` to a
            // metric SRID — both are valid follow-ups. For now we use a degree-radius probe.
            // 0.05° ≈ 5 km at this latitude — generous enough to catch the two Dublin sites.
            near <- listBuildings(BuildingFilterQuery.empty.copy(
              nearLat = Some(DublinCentre.lat),
              nearLon = Some(DublinCentre.lon),
              radiusMeters = Some(0.05)
            ))
            _ = assertEquals(near.map(_.name).toSet, Set("HQ", "Lab"))
            // A very tight radius around Cork should only hit Cork.
            cork <- listBuildings(BuildingFilterQuery.empty.copy(
              nearLat = Some(CorkCity.lat),
              nearLon = Some(CorkCity.lon),
              radiusMeters = Some(0.1)
            ))
            _ = assertEquals(cork.map(_.name), List("Cork-Office"))
          } yield ())
      }
    }
  }

  test("nameContains + ids and partial near* triple — nameContains alone still applies, near* trio drops") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            a <- createBuilding("Atrium", DublinCentre)
            _ <- createBuilding("Lounge", TrinityCollege)
            hits <- listBuildings(
              BuildingFilterQuery.empty.copy(nameContains = Some("atrium"), nearLat = Some(53.0))
            )
            _ = assertEquals(hits.map(_.id), List(a.id))
          } yield ())
      }
    }
  }

  test("404 on a missing building id") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          getByIdReq(UUID.randomUUID).sendResp.map { resp =>
            assertEquals(resp.code, StatusCode.NotFound)
          }
      }
    }
  }
}
