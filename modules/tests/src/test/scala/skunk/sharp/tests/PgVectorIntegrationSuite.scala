package skunk.sharp.tests

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.PostgreSQLContainer
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.{Session, TypingStrategy}
import skunk.sharp.contrib.pgvector.*
import skunk.sharp.dsl.{*, given}
import skunk.sharp.validation.Mismatch

object PgVectorIntegrationSuite {
  case class Chunk(id: Long, doc: String, content: String, embedding: PgVector[3])
  case class NewChunk(doc: String, content: String, embedding: PgVector[3])
  case class WrongDims(id: Long, doc: String, content: String, embedding: PgVector[4])
}

/**
 * pgvector end-to-end. Needs an image with the extension (`pgvector/pgvector:pg18`) and `TypingStrategy.SearchPath`.
 */
class PgVectorIntegrationSuite extends PgFixture {
  import PgVectorIntegrationSuite.*

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(
      dockerImageName = DockerImageName.parse("pgvector/pgvector:pg18").asCompatibleSubstituteFor("postgres"),
      databaseName = "skunk_sharp",
      username = "skunk_sharp",
      password = "skunk_sharp"
    )

  override protected def runMigrations(conn: ConnectionConfig): IO[Unit] =
    Dumbo.withResourcesIn[IO]("migrations-pgvector").apply(conn).runMigration.void

  override def session(containers: containerDef.Container): Resource[IO, Session[IO]] =
    Session
      .Builder[IO]
      .withHost(containers.host)
      .withPort(containers.mappedPort(5432))
      .withUserAndPassword(containers.username, containers.password)
      .withDatabase(containers.databaseName)
      .withTypingStrategy(TypingStrategy.SearchPath)
      .single

  private val chunks = Table.of[Chunk]("chunks").withPrimary("id").withDefault("id")

  // A whole batch of embeddings as ONE typed parameter.
  private val loadBatch =
    chunks.insert.from(Pg.unnestRows[NewChunk]("rows").alias("r").select).compile

  // Top-k by cosine distance within a document, compiled once; runs with (doc = …, query = …).
  private val topK =
    chunks
      .select(c => (c.content, c.embedding.cosineDistance(Param.named["query", PgVector[3]])))
      .where(c => c.doc === Param.named["doc", String])
      .orderBy(c => c.embedding.cosineDistance(Param.named["query", PgVector[3]]).asc)
      .limit(2)
      .compile

  private val rows = List(
    NewChunk("handbook", "east", PgVector[3](1f, 0f, 0f)),
    NewChunk("handbook", "north", PgVector[3](0f, 1f, 0f)),
    NewChunk("handbook", "north-east", PgVector[3](0.7f, 0.7f, 0f)),
    NewChunk("other", "east again", PgVector[3](1f, 0.01f, 0f))
  )

  test("batch insert via unnestRows, then top-k cosine search with a named query vector") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _   <- loadBatch.run(s)((rows = rows))
          hit <- topK.run(s)((query = PgVector[3](0.9f, 0.1f, 0f), doc = "handbook"))
          _ = assertEquals(hit.map(_._1), List("east", "north-east"))
          _ = assert(hit.head._2 < hit(1)._2, hit.toString)
          round <- chunks.select(c => c.embedding).where(c => c.content === "north").compile.unique(s)
          _ = assertEquals(round, PgVector[3](0f, 1f, 0f))
          stats <- chunks
            .select(c => (PgVector.dims(c.embedding), PgVector.norm(c.embedding)))
            .where(c => c.content === "east")
            .compile
            .unique(s)
          _ = assertEquals(stats._1, 3)
          _ = assertEqualsDouble(stats._2, 1.0, 1e-6)
        } yield ()
      }
    }
  }

  test("SchemaValidator matches vector(3) and flags a declared dimension that differs") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ok <- SchemaValidator.validate[IO](s, chunks)
          _ = assert(ok.isValid, ok.mismatches.map(_.pretty).mkString("; "))
          bad <- SchemaValidator.validate[IO](s, Table.of[WrongDims]("chunks"))
          _ = assertEquals(
            bad.mismatches.collect { case m: Mismatch.TypeMismatch => (m.column, m.expected, m.actual) },
            List(("embedding", "vector(4)", "vector(3)"))
          )
        } yield ()
      }
    }
  }
}
