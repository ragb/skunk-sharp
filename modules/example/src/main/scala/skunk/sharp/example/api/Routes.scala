package skunk.sharp.example.api

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
        pool.use(rooms.findById(id).run)
          .map(_.map(_.toResponse).toRight(notFound(s"Room $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.create.serverLogic[IO] { req =>
        pool.use { s =>
          rooms.create(req.toRow).run(s)
            .flatMap(id => rooms.findById(id).run(s).map(_.get.toResponse))
        }
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.patch.serverLogic[IO] { (id, req) =>
        pool.use(rooms.patch(id, req.toRow).run)
          .map(_.map(_.toResponse).toRight(notFound(s"Room $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.delete.serverLogic[IO] { id =>
        pool.use { s =>
          rooms.findById(id).run(s).flatMap {
            case None    => IO.pure(notFound(s"Room $id not found").asLeft[Unit])
            case Some(_) => rooms.delete(id).run(s).as(().asRight[Err])
          }
        }
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
        pool.use(bookings.findById(id).run)
          .map(_.map(_.toResponse).toRight(notFound(s"Booking $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.byRoom.serverLogic[IO] { roomId =>
        Stream.resource(pool).flatMap(bookings.findByRoom(roomId).run).map(_.toResponse).compile.toList
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.create.serverLogic[IO] { req =>
        pool.use { s =>
          bookings.findOverlapping(req.roomId, req.startDate, req.endDate).run(s).flatMap {
            case _ :: _ =>
              IO.pure(conflict("Room already booked during this period").asLeft[BookingResponse])
            case Nil =>
              bookings.create(req.toRow).run(s)
                .flatMap(id => bookings.findById(id).run(s).map(_.get.toResponse))
                .map(_.asRight[Err])
          }
        }
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.delete.serverLogic[IO] { id =>
        pool.use { s =>
          bookings.findById(id).run(s).flatMap {
            case None    => IO.pure(notFound(s"Booking $id not found").asLeft[Unit])
            case Some(_) => bookings.delete(id).run(s).as(().asRight[Err])
          }
        }
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    val swagger = SwaggerInterpreter()
      .fromEndpoints[IO](Endpoints.all, "Room Booking API", "1.0")

    Http4sServerInterpreter[IO]().toRoutes(roomEndpoints ++ bookingEndpoints ++ swagger)
  }

}
