package skunk.sharp.example

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import munit.CatsEffectSuite
import org.http4s.HttpRoutes
import org.http4s.client.Client
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.Session
import skunk.sharp.example.api.Routes
import skunk.sharp.example.repository.{BookingRepository, RoomRepository}
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.{Request, Response, SttpBackend}
import sttp.client3.http4s.Http4sBackend
import sttp.model.Uri as SttpUri
import sttp.tapir.client.sttp.SttpClientInterpreter

/**
 * Integration-test fixture for the example app: spins up Postgres in a container, runs the example's own dumbo
 * migrations, builds the live `Routes`, and exposes an in-process **sttp `SttpBackend[IO, …]`** wired to the route
 * handler via `Http4sBackend.usingClient(Client.fromHttpApp(routes.orNotFound))`.
 *
 * Tests use [[SttpClientInterpreter]] to derive typed sttp requests directly from the same `Endpoints.rooms.list` /
 * `Endpoints.bookings.list` / etc. values the server publishes — the input DTOs (`RoomFilterQuery`,
 * `BookingFilterQuery`, `CreateRoomRequest`, …) flow through unchanged on both sides. No port is bound; everything runs
 * in-process for speed and determinism.
 */
trait ExampleAppFixture extends CatsEffectSuite with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(
      dockerImageName = DockerImageName.parse("postgres:18-alpine"),
      databaseName = "skunk_sharp_example",
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
    Dumbo.withResourcesIn[IO]("migrations").apply(conn).runMigration.void.unsafeRunSync()
  }

  /** A skunk session pool resource against the running container. */
  protected def sessionPool(c: containerDef.Container): Resource[IO, Resource[IO, Session[IO]]] =
    Session
      .Builder[IO]
      .withHost(c.host)
      .withPort(c.mappedPort(5432))
      .withUserAndPassword(c.username, c.password)
      .withDatabase(c.databaseName)
      .pooled(8)

  /**
   * Truncate the example's tables (`bookings`, `rooms`) before each test. `TestContainerForAll` reuses one container
   * across the suite to keep CI fast — but that means rows from one test leak into the next, so tests that assert "all
   * rooms" or expect a specific count must run against an empty database.
   *
   * Uses raw `skunk.command` because the truncate has no DSL representation in this codebase yet (no `Table#truncate`
   * extension); it's a one-shot DDL-ish maintenance op.
   */
  protected def truncateAll(c: containerDef.Container): IO[Unit] = {
    import skunk.implicits.*
    sessionPool(c).use(_.use(_.execute(sql"TRUNCATE TABLE bookings, rooms RESTART IDENTITY CASCADE".command).void))
  }

  /** Base URI used to build sttp requests — all paths are absolute against this. */
  protected val baseUri: SttpUri = sttp.model.Uri.unsafeParse("http://test")

  /** The tapir → sttp interpreter — `toRequestThrowDecodeFailures` returns a function `Input => Request`. */
  protected val interpreter: SttpClientInterpreter = SttpClientInterpreter()

  /**
   * Build an sttp backend wired in-process to the live app's routes. Tests should use this with [[interpreter]] to
   * derive typed requests from the published [[skunk.sharp.example.api.Endpoints]] values.
   */
  protected def appBackend(c: containerDef.Container): Resource[IO, SttpBackend[IO, Fs2Streams[IO]]] =
    sessionPool(c).map { pool =>
      val routes: HttpRoutes[IO] = Routes(pool, RoomRepository.live, BookingRepository.live)
      val client: Client[IO]     = Client.fromHttpApp(routes.orNotFound)
      Http4sBackend.usingClient(client)
    }

  // ---- Request execution helpers (backend as a context parameter) ---------------------------
  //
  // Each suite declares `given SttpBackend[…]` once (typically by binding the lambda parameter via the
  // `case given` pattern), then calls `.sendOk` / `.sendStatus` on tapir-derived requests without
  // threading the backend through every call.

  /**
   * Send a request whose body is a tapir-derived `Either[E, O]` and unwrap the right side, raising on the left. Use for
   * the happy-path assertions where a non-2xx is a test failure. Tapir-derived requests have capability `Any`; the
   * backend's `Fs2Streams[IO] & Effect[IO]` trivially conforms.
   */
  extension [E, O](req: Request[Either[E, O], Any])

    protected def sendOk(using backend: SttpBackend[IO, Fs2Streams[IO]]): IO[O] =
      req.send(backend).flatMap(_.body match {
        case Right(o) => IO.pure(o)
        case Left(e)  => IO.raiseError(new RuntimeException(s"request failed: $e"))
      })

  /** Send and return the full response — for status-code assertions and error-envelope inspection. */
  extension [T](req: Request[T, Any])

    protected def sendResp(using backend: SttpBackend[IO, Fs2Streams[IO]]): IO[Response[T]] =
      req.send(backend)

}
