package skunk.sharp.example.api

import cats.effect.IO
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.StreamBackend
import sttp.model.StatusCode

import java.time.LocalDate
import java.util.UUID

/**
 * `PUT /api/v1/buildings/{buildingId}/rooms` — one MERGE (fed by typed `unnest` array parameters) makes a building's
 * rooms match the request: updates changed capacities, inserts new rooms, deletes missing ones unless they're booked.
 */
class RoomsSyncEndpointSuite extends ExampleAppFixture {

  private val createBuildingReq = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.create, Some(baseUri))
  private val createRoomReq     = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.create, Some(baseUri))
  private val createBookingReq  = interpreter.toRequestThrowDecodeFailures(Endpoints.bookings.create, Some(baseUri))
  private val listReq           = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.list, Some(baseUri))
  private val syncReq           = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.sync, Some(baseUri))
  private val syncRawReq        = interpreter.toRequest(Endpoints.rooms.sync, Some(baseUri))

  private def createBuilding(name: String)(using StreamBackend[IO, Fs2Streams[IO]]): IO[BuildingResponse] =
    createBuildingReq(CreateBuildingRequest(name, s"$name address", LatLon(53.34, -6.26))).sendOk

  private def createRoom(buildingId: UUID, name: String, capacity: Int)(using
    StreamBackend[IO, Fs2Streams[IO]]
  ): IO[RoomResponse] =
    createRoomReq((buildingId, CreateRoomRequest(name, capacity, location = "unsorted", amenities = Map.empty))).sendOk

  private def rooms(buildingId: UUID)(using StreamBackend[IO, Fs2Streams[IO]]): IO[Map[String, Int]] =
    listReq((buildingId, RoomFilterQuery.empty)).sendOk.map(_.map(r => r.name -> r.capacity).toMap)

  private def sync(buildingId: UUID, rs: (String, Int)*)(using
    StreamBackend[IO, Fs2Streams[IO]]
  ): IO[SyncRoomsResponse] =
    syncReq((buildingId, rs.toList.map((n, c) => SyncRoomRequest(n, c)))).sendOk

  test("sync updates changed rooms, inserts new ones, deletes missing ones but keeps booked rooms") {
    withContainers { containers =>
      appBackend(containers).use { case given StreamBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b <- createBuilding("HQ")
            _ <- createRoom(b.id, "A", 2)
            _ <- createRoom(b.id, "B", 4)
            _ <- createRoom(b.id, "C", 6)
            d <- createRoom(b.id, "D", 8)
            _ <- createBookingReq(
              CreateBookingRequest(
                d.id,
                "alice",
                "standup",
                LocalDate.parse("2030-01-01"),
                LocalDate.parse("2030-01-02")
              )
            ).sendOk
            // A unchanged, B resized, E new; C and D missing — C goes, D stays because it's booked.
            res   <- sync(b.id, "A" -> 2, "B" -> 10, "E" -> 3)
            after <- rooms(b.id)
            _ = assertEquals(res, SyncRoomsResponse(inserted = 1, updated = 1, deleted = 1))
            _ = assertEquals(after, Map("A" -> 2, "B" -> 10, "D" -> 8, "E" -> 3))
          } yield ())
      }
    }
  }

  test("sync only touches the target building") {
    withContainers { containers =>
      appBackend(containers).use { case given StreamBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b1  <- createBuilding("one")
            b2  <- createBuilding("two")
            _   <- createRoom(b1.id, "X", 1)
            _   <- createRoom(b2.id, "X", 1)
            res <- sync(b1.id) // empty list: remove every (unbooked) room of b1
            r1  <- rooms(b1.id)
            r2  <- rooms(b2.id)
            _ = assertEquals(res, SyncRoomsResponse(inserted = 0, updated = 0, deleted = 1))
            _ = assertEquals(r1, Map.empty[String, Int])
            _ = assertEquals(r2, Map("X" -> 1))
          } yield ())
      }
    }
  }

  test("duplicate names are rejected with 400; an unknown building is 404") {
    withContainers { containers =>
      appBackend(containers).use { case given StreamBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b    <- createBuilding("HQ")
            dup  <- syncRawReq((b.id, List(SyncRoomRequest("A", 1), SyncRoomRequest("A", 2)))).sendResp
            miss <- syncRawReq((UUID.randomUUID, List(SyncRoomRequest("A", 1)))).sendResp
            _ = assertEquals(dup.code, StatusCode.BadRequest)
            _ = assertEquals(miss.code, StatusCode.NotFound)
          } yield ())
      }
    }
  }
}
