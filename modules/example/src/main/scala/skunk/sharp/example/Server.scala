package skunk.sharp.example

import cats.effect.*
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.{Logger => HttpLogger}
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.sharp.example.api.Routes
import skunk.sharp.example.repository.{BookingRepository, RoomRepository}
import skunk.Session

object Server {

  def run(cfg: AppConfig): IO[Nothing] = {
    val pool = Session.Builder[IO]
      .withHost(cfg.db.host)
      .withPort(cfg.db.port)
      .withUserAndPassword(cfg.db.user, cfg.db.password)
      .withDatabase(cfg.db.database)
      .pooled(cfg.db.maxSessions)

    pool.use { sessionPool =>
      val routes = Routes(sessionPool, RoomRepository.live, BookingRepository.live)
      val logged  = HttpLogger.httpRoutes[IO](logHeaders = false, logBody = false)(routes)

      val host = Host.fromString(cfg.server.host).getOrElse(host"0.0.0.0")
      val port = Port.fromInt(cfg.server.port).getOrElse(port"8080")

      EmberServerBuilder
        .default[IO]
        .withHost(host)
        .withPort(port)
        .withHttpApp(logged.orNotFound)
        .build
        .useForever
    }
  }

}
