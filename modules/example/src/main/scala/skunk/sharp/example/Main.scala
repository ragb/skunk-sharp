package skunk.sharp.example

import cats.effect.*
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given

object Main extends IOApp.Simple {

  def run: IO[Unit] =
    for {
      cfg <- AppConfig.load
      _   <- migrate(cfg.db)
      _   <- Server.run(cfg)
    } yield ()

  private def migrate(db: DbConfig): IO[Unit] = {
    val conn = ConnectionConfig(
      host     = db.host,
      port     = db.port,
      user     = db.user,
      database = db.database,
      password = Some(db.password),
      ssl      = ConnectionConfig.SSL.None
    )
    Dumbo.withResourcesIn[IO]("migrations").apply(conn).runMigration.void
  }

}
