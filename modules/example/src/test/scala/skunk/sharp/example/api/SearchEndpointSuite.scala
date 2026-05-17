package skunk.sharp.example.api

import cats.effect.IO
import cats.syntax.all.*
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend

import java.time.LocalDate
import java.util.UUID

/**
 * Integration tests for the cross-resource search endpoints (`/api/v1/search/...`). Each test composes a multi-table
 * scenario (a building, several rooms, optionally bookings) and verifies the query stitches them together right.
 */
class SearchEndpointSuite extends ExampleAppFixture {

  private val createBuildingReq = interpreter.toRequestThrowDecodeFailures(Endpoints.buildings.create, Some(baseUri))
  private val createRoomReq     = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.create, Some(baseUri))
  private val createBookingReq  = interpreter.toRequestThrowDecodeFailures(Endpoints.bookings.create, Some(baseUri))
  private val searchRoomsReq    = interpreter.toRequestThrowDecodeFailures(Endpoints.search.rooms, Some(baseUri))

  private val availabilityReq =
    interpreter.toRequestThrowDecodeFailures(Endpoints.search.availability, Some(baseUri))

  private val DublinCentre = LatLon(53.3498, -6.2603)
  private val CorkCity     = LatLon(51.8985, -8.4756)

  private def createBuilding(name: String, l: LatLon)(using SttpBackend[IO, Fs2Streams[IO]]): IO[BuildingResponse] =
    createBuildingReq(CreateBuildingRequest(name, address = s"$name address", location = l)).sendOk

  private def createRoom(
    buildingId: UUID,
    name: String,
    capacity: Int,
    location: String = "unsorted",
    amenities: Map[String, String] = Map.empty
  )(using SttpBackend[IO, Fs2Streams[IO]]): IO[RoomResponse] =
    createRoomReq((buildingId, CreateRoomRequest(name, capacity, location, amenities))).sendOk

  private def createBooking(roomId: UUID, from: LocalDate, to: LocalDate)(using
    SttpBackend[IO, Fs2Streams[IO]]
  )
    : IO[BookingResponse] =
    createBookingReq(CreateBookingRequest(roomId, "booker", "title", from, to)).sendOk

  // ---------- /api/v1/search/rooms --------------------------------------------------------------

  test("search/rooms: joins rooms × buildings; spatial filter scopes to the Dublin building") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            dub  <- createBuilding("Dublin HQ", DublinCentre)
            cork <- createBuilding("Cork Office", CorkCity)
            _    <- createRoom(dub.id, "DubRoom1", 4)
            _    <- createRoom(dub.id, "DubRoom2", 8)
            _    <- createRoom(cork.id, "CorkRoom1", 6)
            rs   <- searchRoomsReq(RoomSearchQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05, // ~5 km in degrees at this latitude
              minCapacity = None,
              maxCapacity = None,
              nameContains = None,
              hasAmenity = None,
              locationUnder = None,
              availableFrom = None,
              availableTo = None
            )).sendOk
            _ = assertEquals(rs.map(_.name).toSet, Set("DubRoom1", "DubRoom2"))
            _ = assertEquals(rs.map(_.buildingName).toSet, Set("Dublin HQ"))
          } yield ())
      }
    }
  }

  test("search/rooms: room filters compose with the spatial filter (hasAmenity + locationUnder)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            dub <- createBuilding("Dublin HQ", DublinCentre)
            _   <-
              createRoom(dub.id, "ProjF3", 4, location = "acme.dublin.floor3.r1", amenities = Map("projector" -> "4k"))
            _ <- createRoom(dub.id, "NoProjF3", 4, location = "acme.dublin.floor3.r2", amenities = Map())
            _ <- createRoom(
              dub.id,
              "ProjF2",
              4,
              location = "acme.dublin.floor2.r1",
              amenities = Map("projector" -> "1080p")
            )
            // Combined filter: has a projector AND on floor 3.
            rs <- searchRoomsReq(RoomSearchQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05,
              minCapacity = None,
              maxCapacity = None,
              nameContains = None,
              hasAmenity = Some("projector"),
              locationUnder = Some("acme.dublin.floor3"),
              availableFrom = None,
              availableTo = None
            )).sendOk
            _ = assertEquals(rs.map(_.name), List("ProjF3"))
          } yield ())
      }
    }
  }

  test("search/rooms: availableFrom+availableTo excludes rooms with overlapping bookings (NOT EXISTS)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            dub <- createBuilding("Dublin HQ", DublinCentre)
            r1  <- createRoom(dub.id, "Free", 4)
            r2  <- createRoom(dub.id, "Booked", 4)
            _   <- createBooking(r2.id, LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
            rs  <- searchRoomsReq(RoomSearchQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05,
              minCapacity = None,
              maxCapacity = None,
              nameContains = None,
              hasAmenity = None,
              locationUnder = None,
              availableFrom = Some(LocalDate.parse("2024-06-10")),
              availableTo = Some(LocalDate.parse("2024-06-20"))
            )).sendOk
            _ = assertEquals(rs.map(_.name), List("Free"))
            _ = assertEquals(rs.map(_.id), List(r1.id))
          } yield ())
      }
    }
  }

  // ---------- /api/v1/search/availability -------------------------------------------------------

  test("search/availability: buildings ordered by free-room count descending in the date range") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            // Dublin building: 3 rooms, 1 booked → 2 free.
            dub <- createBuilding("Dublin HQ", DublinCentre)
            _   <- createRoom(dub.id, "D1", 2)
            _   <- createRoom(dub.id, "D2", 2)
            d3  <- createRoom(dub.id, "D3", 2)
            _   <- createBooking(d3.id, LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
            // Trinity building (also Dublin): 1 room, free.
            tri <- createBuilding("Trinity Annex", LatLon(53.3438, -6.2546))
            _   <- createRoom(tri.id, "T1", 2)
            // Cork building: 5 rooms — outside radius, must not appear.
            cork <- createBuilding("Cork Office", CorkCity)
            _    <- (1 to 5).toList.traverse(i => createRoom(cork.id, s"C$i", 2))
            res  <- availabilityReq(AvailabilityQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05,
              from = LocalDate.parse("2024-06-10"),
              to = LocalDate.parse("2024-06-20")
            )).sendOk
            _ = assertEquals(res.map(_.name), List("Dublin HQ", "Trinity Annex"))
            _ = assertEquals(res.map(_.freeRoomCount), List(2L, 1L))
          } yield ())
      }
    }
  }

  test("search/availability: a building with no rooms still shows up with freeRoomCount = 0 (LEFT JOIN)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            _   <- createBuilding("Empty Office", DublinCentre)
            res <- availabilityReq(AvailabilityQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05,
              from = LocalDate.parse("2024-06-10"),
              to = LocalDate.parse("2024-06-20")
            )).sendOk
            _ = assertEquals(res.map(_.name), List("Empty Office"))
            _ = assertEquals(res.map(_.freeRoomCount), List(0L))
          } yield ())
      }
    }
  }

  test("search/availability: a fully booked building reports 0 free rooms") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *>
          (for {
            b   <- createBuilding("Fully Booked", DublinCentre)
            r1  <- createRoom(b.id, "R1", 2)
            r2  <- createRoom(b.id, "R2", 2)
            _   <- createBooking(r1.id, LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
            _   <- createBooking(r2.id, LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
            res <- availabilityReq(AvailabilityQuery(
              nearLat = DublinCentre.lat,
              nearLon = DublinCentre.lon,
              radiusMeters = 0.05,
              from = LocalDate.parse("2024-06-10"),
              to = LocalDate.parse("2024-06-20")
            )).sendOk
            _ = assertEquals(res.map(_.freeRoomCount), List(0L))
          } yield ())
      }
    }
  }
}
