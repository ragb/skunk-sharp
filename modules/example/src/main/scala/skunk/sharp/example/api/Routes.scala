package skunk.sharp.example.api

import cats.data.{EitherT, OptionT}
import cats.effect.IO
import cats.syntax.all.*
import fs2.Stream
import skunk.Session
import sttp.model.StatusCode
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import org.http4s.HttpRoutes
import skunk.sharp.example.repository.{BookingRepository, BuildingRepository, RoomRepository, SearchRepository}
import Transformers.*

object Routes {

  private type Err = (StatusCode, ApiError)
  private def notFound(msg: String): Err   = (StatusCode.NotFound, ApiError(msg))
  private def internal(msg: String): Err   = (StatusCode.InternalServerError, ApiError(msg))
  private def conflict(msg: String): Err   = (StatusCode.Conflict, ApiError(msg))
  private def badRequest(msg: String): Err = (StatusCode.BadRequest, ApiError(msg))

  def apply(
    pool: cats.effect.Resource[IO, Session[IO]],
    buildings: BuildingRepository,
    rooms: RoomRepository,
    bookings: BookingRepository,
    search: SearchRepository
  ): HttpRoutes[IO] = {

    // ---- Buildings ------------------------------------------------------------------------

    val buildingEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.buildings.list.serverLogic[IO] { q =>
        Stream.resource(pool).flatMap(buildings.findFiltered(q.toFilters).run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.buildings.getById.serverLogic[IO] { id =>
        pool.useKleisli(
          EitherT.fromOptionF(buildings.findById(id), notFound(s"Building $id not found"))
            .map(_.toResponse)
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.buildings.create.serverLogic[IO] { req =>
        pool.useKleisli(
          (for {
            id <- EitherT.liftF(buildings.create(req.toRow))
            b  <- EitherT.fromOptionF(buildings.findById(id), internal("building disappeared after create"))
          } yield b.toResponse).value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.buildings.patch.serverLogic[IO] { (id, req) =>
        pool.useKleisli(
          EitherT.fromOptionF(buildings.patch(id, req.toRow), notFound(s"Building $id not found"))
            .map(_.toResponse)
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.buildings.delete.serverLogic[IO] { id =>
        pool.useKleisli(
          OptionT(buildings.findById(id))
            .semiflatMap(_ => buildings.delete(id))
            .toRight(notFound(s"Building $id not found"))
            .void
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    // ---- Rooms (nested under buildings) ---------------------------------------------------

    val roomEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.rooms.list.serverLogic[IO] { case (buildingId, q) =>
        // Ensure the building exists — otherwise it's a 404 even if zero rooms would naturally match.
        pool.useKleisli(
          EitherT.fromOptionF(buildings.findById(buildingId), notFound(s"Building $buildingId not found"))
            .void
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure).flatMap {
          case Left(err) => IO.pure(err.asLeft)
          case Right(()) =>
            q.toFilters match {
              case Left(invalid)  => IO.pure(badRequest(invalid).asLeft)
              case Right(filters) =>
                Stream.resource(pool).flatMap(rooms.findFiltered(buildingId, filters).run).map(_.toResponse)
                  .compile.toList.map(_.asRight[Err])
                  .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
            }
        }
      },
      Endpoints.rooms.getById.serverLogic[IO] { case (buildingId, id) =>
        pool.useKleisli(
          EitherT.fromOptionF(rooms.findById(buildingId, id), notFound(s"Room $id not found in building $buildingId"))
            .map(_.toResponse)
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.rooms.create.serverLogic[IO] { case (buildingId, req) =>
        pool.useKleisli(
          (for {
            _ <- EitherT.fromOptionF(
              buildings.findById(buildingId),
              notFound(s"Building $buildingId not found")
            )
            row  <- EitherT.fromEither[Cats.K](req.toRow(buildingId).leftMap(badRequest))
            id   <- EitherT.liftF[Cats.K, Err, java.util.UUID](rooms.create(row))
            room <- EitherT.fromOptionF(
              rooms.findById(buildingId, id),
              internal("room disappeared after create")
            )
          } yield room.toResponse).value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.rooms.patch.serverLogic[IO] { case (buildingId, id, req) =>
        pool.useKleisli(
          EitherT.fromOptionF(
            rooms.patch(buildingId, id, req.toRow),
            notFound(s"Room $id not found in building $buildingId")
          ).map(_.toResponse).value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.rooms.delete.serverLogic[IO] { case (buildingId, id) =>
        pool.useKleisli(
          OptionT(rooms.findById(buildingId, id))
            .semiflatMap(_ => rooms.delete(buildingId, id))
            .toRight(notFound(s"Room $id not found in building $buildingId"))
            .void
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.rooms.sync.serverLogic[IO] { case (buildingId, req) =>
        // MERGE can't touch the same target row twice, so duplicate names are a client error, not a 500.
        val dupes = req.groupBy(_.name).collect { case (n, rs) if rs.size > 1 => n }.toList.sorted
        if (dupes.nonEmpty) IO.pure(badRequest(s"Duplicate room names: ${dupes.mkString(", ")}").asLeft)
        else
          pool.useKleisli(
            (for {
              _ <- EitherT.fromOptionF(
                buildings.findById(buildingId),
                notFound(s"Building $buildingId not found")
              )
              actions <- EitherT.liftF[Cats.K, Err, List[String]](rooms.sync(buildingId, req.map(_.toRow)))
            } yield SyncRoomsResponse(
              inserted = actions.count(_ == "INSERT"),
              updated = actions.count(_ == "UPDATE"),
              deleted = actions.count(_ == "DELETE")
            )).value
          ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    // ---- Bookings (cross-building, flat) --------------------------------------------------

    val bookingEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.bookings.list.serverLogic[IO] { q =>
        q.toFilters match {
          case Left(invalid)  => IO.pure(badRequest(invalid).asLeft)
          case Right(filters) =>
            Stream.resource(pool).flatMap(bookings.findFiltered(filters).run).map(_.toResponse).compile.toList
              .map(_.asRight[Err])
              .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
        }
      },
      Endpoints.bookings.getById.serverLogic[IO] { id =>
        pool.useKleisli(
          EitherT.fromOptionF(bookings.findById(id), notFound(s"Booking $id not found"))
            .map(_.toResponse)
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.bookings.create.serverLogic[IO] { req =>
        pool.useKleisli(
          (for {
            row <- EitherT.fromEither[Cats.K](req.toRow.leftMap(badRequest))
            _   <- EitherT(
              bookings.findOverlapping(row.room_id, row.period)
                .map(xs => Either.cond(xs.isEmpty, (), conflict("Room already booked during this period")))
            )
            id      <- EitherT.liftF(bookings.create(row))
            booking <- EitherT.fromOptionF(bookings.findById(id), internal("booking disappeared after create"))
          } yield booking.toResponse).value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },
      Endpoints.bookings.delete.serverLogic[IO] { id =>
        pool.useKleisli(
          OptionT(bookings.findById(id))
            .semiflatMap(_ => bookings.delete(id))
            .toRight(notFound(s"Booking $id not found"))
            .void
            .value
        ).handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    // ---- Cross-resource search ------------------------------------------------------------

    val searchEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.search.rooms.serverLogic[IO] { q =>
        (q.toRoomFilters, q.availableDuring).tupled match {
          case Left(invalid)                     => IO.pure(badRequest(invalid).asLeft)
          case Right((filters, availableDuring)) =>
            Stream.resource(pool)
              .flatMap(search.findRoomsNear((q.nearLat, q.nearLon), q.radiusMeters, filters, availableDuring).run)
              .map(_.toResponse)
              .compile.toList
              .map(_.asRight[Err])
              .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
        }
      },
      Endpoints.search.availability.serverLogic[IO] { q =>
        q.period match {
          case Left(invalid) => IO.pure(badRequest(invalid).asLeft)
          case Right(period) =>
            Stream.resource(pool)
              .flatMap(search.buildingsWithAvailability((q.nearLat, q.nearLon), q.radiusMeters, period).run)
              .map(_.toResponse)
              .compile.toList
              .map(_.asRight[Err])
              .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
        }
      }
    )

    val swagger = SwaggerInterpreter()
      .fromEndpoints[IO](Endpoints.all, "Room Booking API", "2.0")

    Http4sServerInterpreter[IO]().toRoutes(
      buildingEndpoints ++ roomEndpoints ++ bookingEndpoints ++ searchEndpoints ++ swagger
    )
  }

}

/** Tiny alias to avoid a long `cats.data.Kleisli`-style type in the for-comprehension. */
private object Cats {
  type K[A] = cats.data.Kleisli[cats.effect.IO, skunk.Session[cats.effect.IO], A]
}
