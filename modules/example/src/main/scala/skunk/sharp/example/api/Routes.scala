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
import skunk.sharp.example.repository.{BookingRepository, RoomRepository}
import Transformers.*

object Routes {

  private type Err = (StatusCode, ApiError)
  private def notFound(msg: String): Err = (StatusCode.NotFound, ApiError(msg))
  private def internal(msg: String): Err = (StatusCode.InternalServerError, ApiError(msg))
  private def conflict(msg: String): Err = (StatusCode.Conflict, ApiError(msg))

  def apply(
    pool: cats.effect.Resource[IO, Session[IO]],
    rooms: RoomRepository,
    bookings: BookingRepository
  ): HttpRoutes[IO] = {

    val roomEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.rooms.list.serverLogic[IO] { _ =>
        Stream.resource(pool).flatMap(rooms.findAll.run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.getById.serverLogic[IO] { id =>
        pool.useKleisli(
          EitherT.fromOptionF(rooms.findById(id), notFound(s"Room $id not found"))
            .map(_.toResponse)
            .value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.create.serverLogic[IO] { req =>
        pool.useKleisli(
          (for {
            id   <- EitherT.liftF(rooms.create(req.toRow))
            room <- EitherT.fromOptionF(rooms.findById(id), internal("room disappeared after create"))
          } yield room.toResponse).value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.patch.serverLogic[IO] { (id, req) =>
        pool.useKleisli(
          EitherT.fromOptionF(rooms.patch(id, req.toRow), notFound(s"Room $id not found"))
            .map(_.toResponse)
            .value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.delete.serverLogic[IO] { id =>
        pool.useKleisli(
          OptionT(rooms.findById(id))
            .semiflatMap(_ => rooms.delete(id))
            .toRight(notFound(s"Room $id not found"))
            .void
            .value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    val bookingEndpoints: List[ServerEndpoint[Any, IO]] = List(
      Endpoints.bookings.list.serverLogic[IO] { _ =>
        Stream.resource(pool).flatMap(bookings.findAll.run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.getById.serverLogic[IO] { id =>
        pool.useKleisli(
          EitherT.fromOptionF(bookings.findById(id), notFound(s"Booking $id not found"))
            .map(_.toResponse)
            .value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.byRoom.serverLogic[IO] { roomId =>
        Stream.resource(pool).flatMap(bookings.findByRoom(roomId).run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.create.serverLogic[IO] { req =>
        pool.useKleisli(
          (for {
            _ <- EitherT(
              bookings.findOverlapping(req.roomId, req.startDate, req.endDate)
                .map(xs => Either.cond(xs.isEmpty, (), conflict("Room already booked during this period")))
            )
            id      <- EitherT.liftF(bookings.create(req.toRow))
            booking <- EitherT.fromOptionF(bookings.findById(id), internal("booking disappeared after create"))
          } yield booking.toResponse).value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.delete.serverLogic[IO] { id =>
        pool.useKleisli(
          OptionT(bookings.findById(id))
            .semiflatMap(_ => bookings.delete(id))
            .toRight(notFound(s"Booking $id not found"))
            .void
            .value
        )
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    val swagger = SwaggerInterpreter()
      .fromEndpoints[IO](Endpoints.all, "Room Booking API", "1.0")

    Http4sServerInterpreter[IO]().toRoutes(roomEndpoints ++ bookingEndpoints ++ swagger)
  }

}
