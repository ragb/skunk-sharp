package skunk.sharp.example

import cats.effect.*
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.{Logger => HttpLogger}
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.sharp.example.api.Routes
import skunk.sharp.example.repository.{BookingRepository, BuildingRepository, RoomRepository, SearchRepository}
import skunk.{Session, TypingStrategy}

object Server {

  def run(cfg: AppConfig): IO[Nothing] = {
    // `TypingStrategy.SearchPath` resolves user-defined types from `pg_type` at session start. The example uses
    // citext / ltree / hstore columns whose OIDs aren't in skunk's built-in oid table, so this is required.
    val pool = Session.Builder[IO]
      .withHost(cfg.db.host)
      .withPort(cfg.db.port)
      .withUserAndPassword(cfg.db.user, cfg.db.password)
      .withDatabase(cfg.db.database)
      .withTypingStrategy(TypingStrategy.SearchPath)
      .pooled(cfg.db.maxSessions)

    pool.use { sessionPool =>
      val routes = Routes(
        sessionPool,
        BuildingRepository.live,
        RoomRepository.live,
        BookingRepository.live,
        SearchRepository.live
      )
      val logged = HttpLogger.httpRoutes[IO](logHeaders = false, logBody = false)(routes)

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
