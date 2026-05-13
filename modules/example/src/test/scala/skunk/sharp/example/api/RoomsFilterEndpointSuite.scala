package skunk.sharp.example.api

import cats.effect.IO
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend
import sttp.model.StatusCode

import java.util.UUID

/**
 * End-to-end test for the nested rooms resource at `GET /api/v1/buildings/{buildingId}/rooms`. Spins up the live
 * fixture, creates a building, then exercises the filter combinations through tapir-derived sttp requests.
 *
 * The point is to verify the full path: tapir's `EndpointInput.derived[RoomFilterQuery]` correctly extracts each query
 * param shape on the server (`Option[T]`, repeated `List[T]`), and the *same* endpoint value on the client side
 * rebuilds those query params from the typed DTO — both sides exchange `RoomFilterQuery` values, no string-keying.
 *
 * The `SttpBackend` is bound as a `given` per test via `case given …` in the resource-`use` lambda.
 */
class RoomsFilterEndpointSuite extends ExampleAppFixture {

  private val createBuildingReq = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.create, Some(baseUri))
  private val createRoomReq     = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.create, Some(baseUri))
  private val listReq           = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.list, Some(baseUri))
  private val getByIdReq        = interpreter.toRequest(Endpoints.rooms.getById, Some(baseUri))

  private def createBuilding(name: String = "HQ", lat: Double = 53.34, lon: Double = -6.26)(using
    SttpBackend[IO, Fs2Streams[IO]]
  ): IO[BuildingResponse] =
    createBuildingReq(CreateBuildingRequest(name, address = s"$name address", location = LatLon(lat, lon))).sendOk

  private def createRoom(
    buildingId: UUID,
    name: String,
    capacity: Int,
    location: String = "unsorted",
    amenities: Map[String, String] = Map.empty
  )(using SttpBackend[IO, Fs2Streams[IO]]): IO[RoomResponse] =
    createRoomReq((buildingId, CreateRoomRequest(name, capacity, location, amenities))).sendOk

  private def listRooms(buildingId: UUID, q: RoomFilterQuery)(using SttpBackend[IO, Fs2Streams[IO]])
    : IO[List[RoomResponse]] = listReq((buildingId, q)).sendOk

  test("filter rooms by minCapacity / maxCapacity within a building") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b   <- createBuilding()
            _   <- createRoom(b.id, "small", 4)
            _   <- createRoom(b.id, "medium", 12)
            _   <- createRoom(b.id, "large", 50)
            all <- listRooms(b.id, RoomFilterQuery.empty)
            _ = assertEquals(all.map(_.name).toSet, Set("small", "medium", "large"))
            big <- listRooms(b.id, RoomFilterQuery.empty.copy(minCapacity = Some(10)))
            _ = assertEquals(big.map(_.name).toSet, Set("medium", "large"))
            mid <- listRooms(b.id, RoomFilterQuery.empty.copy(minCapacity = Some(5), maxCapacity = Some(20)))
            _ = assertEquals(mid.map(_.name).toSet, Set("medium"))
          } yield ())
      }
    }
  }

  test("filter rooms by nameContains (ILIKE substring)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b     <- createBuilding()
            _     <- createRoom(b.id, "Atrium-A", 8)
            _     <- createRoom(b.id, "Atrium-B", 8)
            _     <- createRoom(b.id, "Lounge", 4)
            atria <- listRooms(b.id, RoomFilterQuery.empty.copy(nameContains = Some("atrium")))
            _ = assertEquals(atria.map(_.name).toSet, Set("Atrium-A", "Atrium-B"))
            lounges <- listRooms(b.id, RoomFilterQuery.empty.copy(nameContains = Some("lou")))
            _ = assertEquals(lounges.map(_.name), List("Lounge"))
          } yield ())
      }
    }
  }

  test("rooms are scoped to their building — querying a different building doesn't leak them") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            ba <- createBuilding("A")
            bb <- createBuilding("B")
            _  <- createRoom(ba.id, "room-in-A", 2)
            _  <- createRoom(bb.id, "room-in-B", 2)
            inA <- listRooms(ba.id, RoomFilterQuery.empty)
            inB <- listRooms(bb.id, RoomFilterQuery.empty)
            _ = assertEquals(inA.map(_.name), List("room-in-A"))
            _ = assertEquals(inB.map(_.name), List("room-in-B"))
          } yield ())
      }
    }
  }

  test("filter rooms by `locationUnder` (ltree descendant query)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b    <- createBuilding()
            _    <- createRoom(b.id, "dub-1", 4, location = "acme.dublin.floor3.r1")
            _    <- createRoom(b.id, "dub-2", 4, location = "acme.dublin.floor2.r1")
            _    <- createRoom(b.id, "cork-1", 4, location = "acme.cork.floor1.r1")
            dub  <- listRooms(b.id, RoomFilterQuery.empty.copy(locationUnder = Some("acme.dublin")))
            _ = assertEquals(dub.map(_.name).toSet, Set("dub-1", "dub-2"))
            f3   <- listRooms(b.id, RoomFilterQuery.empty.copy(locationUnder = Some("acme.dublin.floor3")))
            _ = assertEquals(f3.map(_.name), List("dub-1"))
          } yield ())
      }
    }
  }

  test("filter rooms by `hasAmenity` (hstore key existence) — and round-trip amenities through the response") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b <- createBuilding()
            _ <- createRoom(b.id, "A", 4, amenities = Map("projector" -> "4k", "whiteboard" -> "true"))
            _ <- createRoom(b.id, "B", 4, amenities = Map("whiteboard" -> "true"))
            _ <- createRoom(b.id, "C", 4, amenities = Map.empty)
            proj <- listRooms(b.id, RoomFilterQuery.empty.copy(hasAmenity = Some("projector")))
            _ = assertEquals(proj.map(_.name), List("A"))
            wb <- listRooms(b.id, RoomFilterQuery.empty.copy(hasAmenity = Some("whiteboard")))
            _ = assertEquals(wb.map(_.name).toSet, Set("A", "B"))
            _ = assertEquals(proj.head.amenities, Map("projector" -> "4k", "whiteboard" -> "true"))
          } yield ())
      }
    }
  }

  test("404 on a missing room id surfaces as a NotFound status with the JSON error envelope") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          createBuilding().flatMap { b =>
            getByIdReq((b.id, UUID.randomUUID)).sendResp.map { resp =>
              assertEquals(resp.code, StatusCode.NotFound)
            }
          }
      }
    }
  }

  test("404 when listing rooms for a non-existent building") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          listReq((UUID.randomUUID, RoomFilterQuery.empty)).sendResp.map { resp =>
            assertEquals(resp.code, StatusCode.NotFound)
          }
      }
    }
  }
}
