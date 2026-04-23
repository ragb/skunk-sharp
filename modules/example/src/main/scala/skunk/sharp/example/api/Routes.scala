package skunk.sharp.example.api

import cats.data.Kleisli
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
        pool.useKleisli(rooms.findById(id))
          .map(_.toRight(notFound(s"Room $id not found")).map(_.toResponse))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.create.serverLogic[IO] { req =>
        pool.useKleisli(for {
          id   <- rooms.create(req.toRow)
          room <- rooms.findById(id)
        } yield room.get.toResponse.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.patch.serverLogic[IO] { (id, req) =>
        pool.useKleisli(rooms.patch(id, req.toRow))
          .map(_.toRight(notFound(s"Room $id not found")).map(_.toResponse))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.delete.serverLogic[IO] { id =>
        pool.useKleisli(for {
          opt    <- rooms.findById(id)
          result <- opt.traverse(_ => rooms.delete(id))
        } yield result.toRight(notFound(s"Room $id not found")).void)
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
        pool.useKleisli(bookings.findById(id))
          .map(_.toRight(notFound(s"Booking $id not found")).map(_.toResponse))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.byRoom.serverLogic[IO] { roomId =>
        Stream.resource(pool).flatMap(bookings.findByRoom(roomId).run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.create.serverLogic[IO] { req =>
        pool.useKleisli(for {
          overlapping <- bookings.findOverlapping(req.roomId, req.startDate, req.endDate)
          result <- overlapping match {
            case _ :: _ =>
              Kleisli.liftF[IO, Session[IO], Either[Err, BookingResponse]](
                IO.pure(conflict("Room already booked during this period").asLeft)
              )
            case Nil =>
              for {
                id      <- bookings.create(req.toRow)
                booking <- bookings.findById(id)
              } yield booking.get.toResponse.asRight[Err]
          }
        } yield result)
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.delete.serverLogic[IO] { id =>
        pool.useKleisli(for {
          opt    <- bookings.findById(id)
          result <- opt.traverse(_ => bookings.delete(id))
        } yield result.toRight(notFound(s"Booking $id not found")).void)
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    val swagger = SwaggerInterpreter()
      .fromEndpoints[IO](Endpoints.all, "Room Booking API", "1.0")

    Http4sServerInterpreter[IO]().toRoutes(roomEndpoints ++ bookingEndpoints ++ swagger)
  }

}
