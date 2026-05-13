package skunk.sharp.example.api

import cats.effect.IO
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend
import sttp.model.StatusCode

import java.util.UUID

/**
 * End-to-end test for `GET /api/v1/rooms` with the dynamic-filter query DTO. Spins up Postgres + the live app via
 * [[ExampleAppFixture]], creates a known fixture set of rooms via `POST`, then exercises filter combinations through
 * tapir-derived sttp requests against the in-process backend.
 *
 * The point is to verify the full path: tapir's `EndpointInput.derived[RoomFilterQuery]` correctly extracts each query
 * param shape on the *server* (`Option[T]`, repeated `List[T]`), and the *same* endpoint value on the *client* side
 * rebuilds those query params from the typed DTO via `SttpClientInterpreter` — both sides exchange `RoomFilterQuery`
 * values, no string-keying anywhere.
 *
 * The `SttpBackend` is bound as a `given` per test via `case given …` in the resource-`use` lambda; the helper methods
 * take it as a context parameter so call sites stay close to "just call the endpoint".
 */
class RoomsFilterEndpointSuite extends ExampleAppFixture {

  private val createRoomReq = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.create, Some(baseUri))
  private val listReq       = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.list, Some(baseUri))
  private val getByIdReq    = interpreter.toRequest(Endpoints.rooms.getById, Some(baseUri))

  private def createRoom(
    name: String,
    capacity: Int,
    location: String = "unsorted",
    amenities: Map[String, String] = Map.empty
  )(using SttpBackend[IO, Fs2Streams[IO]]): IO[RoomResponse] =
    createRoomReq(CreateRoomRequest(name, capacity, location, amenities)).sendOk

  private def listRooms(q: RoomFilterQuery)(using SttpBackend[IO, Fs2Streams[IO]]): IO[List[RoomResponse]] =
    listReq(q).sendOk

  test("filter rooms by minCapacity / maxCapacity") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _   <- createRoom("small", 4)
            _   <- createRoom("medium", 12)
            _   <- createRoom("large", 50)
            all <- listRooms(RoomFilterQuery.empty)
            _ = assertEquals(all.map(_.name).toSet, Set("small", "medium", "large"))
            big <- listRooms(RoomFilterQuery.empty.copy(minCapacity = Some(10)))
            _ = assertEquals(big.map(_.name).toSet, Set("medium", "large"))
            mid <- listRooms(RoomFilterQuery.empty.copy(minCapacity = Some(5), maxCapacity = Some(20)))
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
            _     <- createRoom("Atrium-A", 8)
            _     <- createRoom("Atrium-B", 8)
            _     <- createRoom("Lounge", 4)
            atria <- listRooms(RoomFilterQuery.empty.copy(nameContains = Some("atrium")))
            _ = assertEquals(atria.map(_.name).toSet, Set("Atrium-A", "Atrium-B"))
            lounges <- listRooms(RoomFilterQuery.empty.copy(nameContains = Some("lou")))
            _ = assertEquals(lounges.map(_.name), List("Lounge"))
          } yield ())
      }
    }
  }

  test("filter rooms by repeated `names` query param (IN)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _  <- createRoom("alpha", 2)
            _  <- createRoom("beta", 2)
            _  <- createRoom("gamma", 2)
            rs <- listRooms(RoomFilterQuery.empty.copy(names = List("alpha", "gamma")))
            _ = assertEquals(rs.map(_.name).toSet, Set("alpha", "gamma"))
          } yield ())
      }
    }
  }

  test("filter rooms by `ids` (IN) — restrict the listing to a known UUID set") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            a  <- createRoom("A", 1)
            b  <- createRoom("B", 2)
            _  <- createRoom("C", 3)
            rs <- listRooms(RoomFilterQuery.empty.copy(ids = List(a.id, b.id)))
            _ = assertEquals(rs.map(_.name).toSet, Set("A", "B"))
          } yield ())
      }
    }
  }

  test("AND-combination — minCapacity + nameContains + names list") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _  <- createRoom("Hub-North", 30)
            _  <- createRoom("Hub-South", 5)
            _  <- createRoom("Hub-East", 30)
            _  <- createRoom("Plaza", 40)
            rs <- listRooms(
              RoomFilterQuery.empty.copy(
                minCapacity = Some(10),
                nameContains = Some("hub"),
                names = List("Hub-North", "Hub-East")
              )
            )
            _ = assertEquals(rs.map(_.name).toSet, Set("Hub-North", "Hub-East"))
          } yield ())
      }
    }
  }

  test("empty filter set returns the static `findAllQ` path — all rooms") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _   <- createRoom("x1", 1)
            _   <- createRoom("x2", 1)
            all <- listRooms(RoomFilterQuery.empty)
            _ = assertEquals(all.size, 2)
          } yield ())
      }
    }
  }

  test("filter rooms by `locationUnder` (ltree descendant query)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _    <- createRoom("dub-1", 4, location = "acme.dublin.floor3.r1")
            _    <- createRoom("dub-2", 4, location = "acme.dublin.floor2.r1")
            _    <- createRoom("cork-1", 4, location = "acme.cork.floor1.r1")
            // Whole Dublin tree (`acme.dublin` ≤ floor3.r1 and floor2.r1; not Cork).
            dub  <- listRooms(RoomFilterQuery.empty.copy(locationUnder = Some("acme.dublin")))
            _ = assertEquals(dub.map(_.name).toSet, Set("dub-1", "dub-2"))
            // One floor only.
            f3   <- listRooms(RoomFilterQuery.empty.copy(locationUnder = Some("acme.dublin.floor3")))
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
            _ <- createRoom("A", 4, amenities = Map("projector" -> "4k", "whiteboard" -> "true"))
            _ <- createRoom("B", 4, amenities = Map("whiteboard" -> "true"))
            _ <- createRoom("C", 4, amenities = Map.empty)
            // Only A has a projector key.
            proj <- listRooms(RoomFilterQuery.empty.copy(hasAmenity = Some("projector")))
            _ = assertEquals(proj.map(_.name), List("A"))
            // Two rooms have a whiteboard.
            wb <- listRooms(RoomFilterQuery.empty.copy(hasAmenity = Some("whiteboard")))
            _ = assertEquals(wb.map(_.name).toSet, Set("A", "B"))
            // The map travels back through the response intact.
            _ = assertEquals(proj.head.amenities, Map("projector" -> "4k", "whiteboard" -> "true"))
          } yield ())
      }
    }
  }

  test("404 on a missing room id surfaces as a NotFound status with the JSON error envelope") {
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
