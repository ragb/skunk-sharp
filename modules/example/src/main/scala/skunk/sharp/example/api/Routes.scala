package skunk.sharp.example.api

import cats.effect.IO
import cats.syntax.all.*
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
        pool.use(s => rooms.findAll(s))
          .map(_.map(_.toResponse).asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.getById.serverLogic[IO] { id =>
        pool.use(s => rooms.findById(id, s))
          .map(_.map(_.toResponse).toRight(notFound(s"Room $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.create.serverLogic[IO] { req =>
        pool.use { s =>
          rooms.create(req.name, req.capacity, s)
            .flatMap(id => rooms.findById(id, s).map(_.get.toResponse))
        }
          .map(_.asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.patch.serverLogic[IO] { (id, req) =>
        pool.use(s => rooms.patch(id, req.name, req.capacity, s))
          .map(_.map(_.toResponse).toRight(notFound(s"Room $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.rooms.delete.serverLogic[IO] { id =>
        pool.use { s =>
          rooms.findById(id, s).flatMap {
            case None    => notFound(s"Room $id not found").asLeft[Unit].pure
            case Some(_) => rooms.delete(id, s).as(().asRight[Err])
          }
        }
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      }
    )

    val bookingEndpoints: List[ServerEndpoint[Any, IO]] = List(

      Endpoints.bookings.list.serverLogic[IO] { _ =>
        pool.use(s => bookings.findAll(s))
          .map(_.map(_.toResponse).asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.getById.serverLogic[IO] { id =>
        pool.use(s => bookings.findById(id, s))
          .map(_.map(_.toResponse).toRight(notFound(s"Booking $id not found")))
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.byRoom.serverLogic[IO] { roomId =>
        pool.use(s => bookings.findByRoom(roomId, s))
          .map(_.map(_.toResponse).asRight[Err])
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.create.serverLogic[IO] { req =>
        pool.use { s =>
          bookings.findOverlapping(req.roomId, req.startDate, req.endDate, s).flatMap {
            case _ :: _ =>
              conflict("Room already booked during this period").asLeft[BookingResponse].pure
            case Nil =>
              bookings.create(req.roomId, req.bookerName, req.title, req.startDate, req.endDate, s)
                .flatMap(id => bookings.findById(id, s).map(_.get.toResponse))
                .map(_.asRight[Err])
          }
        }
          .handleErrorWith(e => internal(e.getMessage).asLeft.pure)
      },

      Endpoints.bookings.delete.serverLogic[IO] { id =>
        pool.use { s =>
          bookings.findById(id, s).flatMap {
            case None    => notFound(s"Booking $id not found").asLeft[Unit].pure
            case Some(_) => bookings.delete(id, s).as(().asRight[Err])
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
