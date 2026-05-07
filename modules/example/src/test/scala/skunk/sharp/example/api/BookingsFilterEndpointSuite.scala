package skunk.sharp.example.api

import cats.effect.IO
import skunk.sharp.example.ExampleAppFixture
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.SttpBackend

import java.time.LocalDate
import java.util.UUID

/**
 * End-to-end test for `GET /api/v1/bookings`. Mirrors [[RoomsFilterEndpointSuite]] but exercises the
 * date-range filter cases — `OverlapsPeriod` (paired bounds), `StartsOnOrAfter` / `EndsOnOrBefore`
 * (half-bounded), `RoomsIn`, `BookerNameContains`, `TitleContains`. The repository materialises these
 * via skunk-sharp's range operators (`&&`, `<@`); this test covers the full path back to JSON responses.
 */
class BookingsFilterEndpointSuite extends ExampleAppFixture {

  private val createRoomReq    = interpreter.toRequestThrowDecodeFailures(Endpoints.rooms.create,    Some(baseUri))
  private val createBookingReq = interpreter.toRequestThrowDecodeFailures(Endpoints.bookings.create, Some(baseUri))
  private val listBookingsReq  = interpreter.toRequestThrowDecodeFailures(Endpoints.bookings.list,   Some(baseUri))

  private def createRoom(name: String, capacity: Int)(using SttpBackend[IO, Fs2Streams[IO]]): IO[RoomResponse] =
    createRoomReq(CreateRoomRequest(name, capacity)).sendOk

  private def createBooking(
    roomId: UUID, booker: String, title: String, from: LocalDate, to: LocalDate
  )(using SttpBackend[IO, Fs2Streams[IO]]): IO[BookingResponse] =
    createBookingReq(CreateBookingRequest(roomId, booker, title, from, to)).sendOk

  private def listBookings(q: BookingFilterQuery)(using SttpBackend[IO, Fs2Streams[IO]]): IO[List[BookingResponse]] =
    listBookingsReq(q).sendOk

  test("filter bookings by `roomIds` (IN)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          a <- createRoom("A", 2)
          b <- createRoom("B", 2)
          c <- createRoom("C", 2)
          _ <- createBooking(a.id, "alice", "ax", LocalDate.parse("2024-01-01"), LocalDate.parse("2024-01-10"))
          _ <- createBooking(b.id, "bob", "bx", LocalDate.parse("2024-02-01"), LocalDate.parse("2024-02-10"))
          _ <- createBooking(c.id, "carol", "cx", LocalDate.parse("2024-03-01"), LocalDate.parse("2024-03-10"))
          rs <- listBookings(BookingFilterQuery.empty.copy(roomIds = List(a.id, b.id)))
          _ = assertEquals(rs.map(_.title).toSet, Set("ax", "bx"))
        } yield ())
      }
    }
  }

  test("filter bookings by bookerNameContains and titleContains (case-insensitive)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          r <- createRoom("only", 1)
          _ <- createBooking(r.id, "Alice Smith", "team standup", LocalDate.parse("2024-01-01"), LocalDate.parse("2024-01-02"))
          _ <- createBooking(r.id, "Bob Jones",   "1:1 sync",     LocalDate.parse("2024-01-03"), LocalDate.parse("2024-01-04"))
          _ <- createBooking(r.id, "Alice Watt",  "design review",LocalDate.parse("2024-01-05"), LocalDate.parse("2024-01-06"))
          alices <- listBookings(BookingFilterQuery.empty.copy(bookerNameContains = Some("alice")))
          _ = assertEquals(alices.map(_.bookerName).toSet, Set("Alice Smith", "Alice Watt"))
          syncs <- listBookings(BookingFilterQuery.empty.copy(titleContains = Some("sync")))
          _ = assertEquals(syncs.map(_.title), List("1:1 sync"))
        } yield ())
      }
    }
  }

  test("filter bookings by overlapsPeriod (paired bounds — `period && [from, to)`)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          r <- createRoom("ovr", 1)
          _ <- createBooking(r.id, "x", "ancient", LocalDate.parse("2000-01-01"), LocalDate.parse("2000-01-31"))
          _ <- createBooking(r.id, "x", "fresh",   LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
          _ <- createBooking(r.id, "x", "future",  LocalDate.parse("2030-01-01"), LocalDate.parse("2030-01-31"))
          probe = BookingFilterQuery.empty.copy(
            overlapsFrom = Some(LocalDate.parse("2024-01-01")),
            overlapsTo   = Some(LocalDate.parse("2024-12-31"))
          )
          hits <- listBookings(probe)
          _ = assertEquals(hits.map(_.title), List("fresh"))
        } yield ())
      }
    }
  }

  test("filter bookings by startsOnOrAfter (half-bounded — `period <@ [date, +∞)`)") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          r <- createRoom("sb", 1)
          _ <- createBooking(r.id, "x", "early",  LocalDate.parse("2024-01-01"), LocalDate.parse("2024-01-31"))
          _ <- createBooking(r.id, "x", "middle", LocalDate.parse("2024-06-01"), LocalDate.parse("2024-06-30"))
          _ <- createBooking(r.id, "x", "late",   LocalDate.parse("2024-12-01"), LocalDate.parse("2024-12-31"))
          rs <- listBookings(
                  BookingFilterQuery.empty.copy(startsOnOrAfter = Some(LocalDate.parse("2024-06-01")))
                )
          _ = assertEquals(rs.map(_.title).toSet, Set("middle", "late"))
        } yield ())
      }
    }
  }

  test("AND-combination — roomIds + overlapsPeriod + bookerNameContains") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          a <- createRoom("RA", 1)
          b <- createRoom("RB", 1)
          // In room A: alice with booking that overlaps 2024-Q2 — match
          _ <- createBooking(a.id, "alice", "match",       LocalDate.parse("2024-04-01"), LocalDate.parse("2024-04-15"))
          // In room A: bob with booking that overlaps — wrong booker, no match
          _ <- createBooking(a.id, "bob",   "wrong-booker",LocalDate.parse("2024-05-01"), LocalDate.parse("2024-05-15"))
          // In room A: alice in Q1 — wrong period, no match
          _ <- createBooking(a.id, "alice", "wrong-period",LocalDate.parse("2024-01-10"), LocalDate.parse("2024-01-20"))
          // In room B: alice overlapping — wrong room, no match
          _ <- createBooking(b.id, "alice", "wrong-room",  LocalDate.parse("2024-04-10"), LocalDate.parse("2024-04-20"))
          rs <- listBookings(
                  BookingFilterQuery.empty.copy(
                    roomIds            = List(a.id),
                    bookerNameContains = Some("alice"),
                    overlapsFrom       = Some(LocalDate.parse("2024-04-01")),
                    overlapsTo         = Some(LocalDate.parse("2024-06-30"))
                  )
                )
          _ = assertEquals(rs.map(_.title), List("match"))
        } yield ())
      }
    }
  }

  test("empty filter set returns the static `findAllQ` path — all bookings") {
    withContainers { containers =>
      appBackend(containers).use { case given SttpBackend[IO, Fs2Streams[IO]] =>
        truncateAll(containers) *> (for {
          r <- createRoom("e", 1)
          _ <- createBooking(r.id, "x", "b1", LocalDate.parse("2024-01-01"), LocalDate.parse("2024-01-02"))
          _ <- createBooking(r.id, "x", "b2", LocalDate.parse("2024-02-01"), LocalDate.parse("2024-02-02"))
          all <- listBookings(BookingFilterQuery.empty)
          _ = assertEquals(all.size, 2)
        } yield ())
      }
    }
  }
}
