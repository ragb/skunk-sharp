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
 * Shared fixture: one Postgres container per suite; the container is migrated via dumbo before any tests see the
 * session. Tests receive a `Resource[IO, Session[IO]]` for per-test sessions.
 *
 * The migration step is overridable per suite — override [[runMigrations]] to point at a different classpath resource
 * dir, run extra setup, etc. `Dumbo.withResourcesIn` is an `inline` macro that needs its path as a compile-time
 * constant, which is why subclasses override the whole method rather than just a `String` field.
 */
trait PgFixture extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(
      dockerImageName = DockerImageName.parse("postgres:18-alpine"),
      databaseName = "skunk_sharp",
      username = "skunk_sharp",
      password = "skunk_sharp"
    )

  /**
   * Run whatever schema-setup the suite needs against the freshly-started container. The default runs dumbo against the
   * `"migrations"` resource directory (the shared set of migrations most suites use).
   *
   * Override to point dumbo at a different directory — the path must be a literal String because `withResourcesIn` is
   * inlined at the call site. Typical override:
   *
   * {{{
   *   override protected def runMigrations(conn: ConnectionConfig): IO[Unit] =
   *     Dumbo.withResourcesIn[IO]("migrations-multischema").apply(conn).runMigration.void
   * }}}
   */
  protected def runMigrations(conn: ConnectionConfig): IO[Unit] =
    Dumbo.withResourcesIn[IO]("migrations").apply(conn).runMigration.void

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
    runMigrations(conn).unsafeRunSync()
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
