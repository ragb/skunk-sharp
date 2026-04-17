package skunk.sharp.tests

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.Session

/**
 * Shared fixture: one Postgres container per suite; dumbo runs migrations from `resources/migrations/` before any tests
 * see the session. Tests receive a `Resource[IO, Session[IO]]` for per-test sessions.
 */
trait PgFixture extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(
      dockerImageName = DockerImageName.parse("postgres:18-alpine"),
      databaseName = "skunk_sharp",
      username = "skunk_sharp",
      password = "skunk_sharp"
    )

  override def afterContainersStart(containers: containerDef.Container): Unit = {
    super.afterContainersStart(containers)
    val conn = ConnectionConfig(
      host = containers.host,
      port = containers.mappedPort(5432),
      user = containers.username,
      database = containers.databaseName,
      password = Some(containers.password),
      ssl = ConnectionConfig.SSL.None
    )
    import cats.effect.unsafe.implicits.global
    Dumbo
      .withResourcesIn[IO]("migrations")
      .apply(conn)
      .runMigration
      .void
      .unsafeRunSync()
  }

  /** Build a skunk session resource against the container. */
  def session(containers: containerDef.Container): Resource[IO, Session[IO]] =
    Session
      .Builder[IO]
      .withHost(containers.host)
      .withPort(containers.mappedPort(5432))
      .withUserAndPassword(containers.username, containers.password)
      .withDatabase(containers.databaseName)
      .single

}
