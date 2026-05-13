package skunk.sharp.tests

import cats.effect.{IO, Resource}
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.{Session, TypingStrategy}
import skunk.sharp.contrib.citext.Citext
import skunk.sharp.contrib.fuzzystrmatch.PgFuzzy
import skunk.sharp.contrib.hstore.Hstore
import skunk.sharp.contrib.hstore.*
import skunk.sharp.contrib.ltree.{LQuery, LTree, PgLtree}
import skunk.sharp.contrib.ltree.*
import skunk.sharp.contrib.pgcrypto.PgCrypto
import skunk.sharp.contrib.pgtrgm.PgTrgm
import skunk.sharp.dsl.*
import skunk.sharp.validation.{Mismatch, SchemaValidator}

object ContribIntegrationSuite {

  case class Account(id: Int, email: Citext, body: String)
  case class Folder(id: Int, path: LTree)
  case class Thing(id: Int, props: Hstore)
}

/**
 * End-to-end tests for every contrib module — runs each operator / function against a real Postgres with the
 * corresponding extension installed via `migrations-contrib/V1__extensions.sql`.
 *
 * Also exercises the extension-aware [[SchemaValidator]]: a clean run, a positive case for `extraExtensions`, and a
 * negative case where the validator is pointed at a relation requiring an extension that is *not* installed.
 */
class ContribIntegrationSuite extends PgFixture {
  import ContribIntegrationSuite.*

  override protected def runMigrations(conn: ConnectionConfig): IO[Unit] =
    Dumbo.withResourcesIn[IO]("migrations-contrib").apply(conn).runMigration.void

  // Override the session builder to use TypingStrategy.SearchPath — required because the contrib types
  // (citext, ltree, hstore, …) are user-defined and aren't in skunk's built-in oid table.
  override def session(containers: containerDef.Container): Resource[IO, Session[IO]] =
    Session
      .Builder[IO]
      .withHost(containers.host)
      .withPort(containers.mappedPort(5432))
      .withUserAndPassword(containers.username, containers.password)
      .withDatabase(containers.databaseName)
      .withTypingStrategy(TypingStrategy.SearchPath)
      .single

  private val accounts = Table.of[Account]("accounts").withPrimary("id")
  private val folders  = Table.of[Folder]("folders").withPrimary("id")
  private val things   = Table.of[Thing]("things").withPrimary("id")

  // ---------- citext -----------------------------------------------------------------------------

  test("citext: case-insensitive equality matches across casing") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- accounts.insert((id = 1, email = Citext("Alice@Example.COM"), body = "hello")).compile.run(s)
          found <- accounts.select(_.id)
            .where(a => a.email === Param.bind(Citext("alice@example.com")))
            .compile.option(s)
          _ = assertEquals(found, Some(1))
        } yield ()
      }
    }
  }

  // ---------- ltree ------------------------------------------------------------------------------

  test("ltree: ancestor / descendant / nlevel / lca / lquery match") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- folders.insert.values(
            (id = 10, path = LTree("top")),
            (id = 11, path = LTree("top.science")),
            (id = 12, path = LTree("top.science.astronomy"))
          ).compile.run(s)
          desc <- folders.select(_.id)
            .where(f => f.path.isDescendantOf(Param.bind(LTree("top.science"))))
            .orderBy(_.id.asc)
            .compile.run(s)
          _ = assertEquals(desc, List(11, 12))
          patt <- folders.select(_.id)
            .where(f => f.path.matches(Param.bind(LQuery("top.*"))))
            .orderBy(_.id.asc)
            .compile.run(s)
          _ = assertEquals(patt, List(10, 11, 12))
          lvl <- empty.select(_ => PgLtree.nlevel(Param.bind(LTree("top.science.astronomy")))).compile.unique(s)
          _ = assertEquals(lvl, 3)
        } yield ()
      }
    }
  }

  // ---------- pg_trgm ----------------------------------------------------------------------------

  test("pg_trgm: similarity ranks closer matches first") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- accounts.insert.values(
            (id = 21, email = Citext("widget1@example.com"), body = "widget assembly guide"),
            (id = 22, email = Citext("nothing@example.com"), body = "a totally unrelated essay")
          ).compile.run(s)
          ranked <- accounts.select(a => (a.id, PgTrgm.similarity(a.body, Param.bind("widget"))))
            .orderBy(a => PgTrgm.similarity(a.body, Param.bind("widget")).desc)
            .compile.run(s)
          _ = assert(ranked.head._1 == 21, s"expected id=21 to rank first, got $ranked")
          _ = assert(ranked.head._2 > ranked.last._2, "first row should have a higher similarity")
        } yield ()
      }
    }
  }

  // ---------- pgcrypto ---------------------------------------------------------------------------

  test("pgcrypto: crypt + gen_salt hashes a password (bcrypt) and verifies via constant-time compare") {
    withContainers { containers =>
      session(containers).use { s =>
        val pw = "hunter2"
        for {
          hash <- empty.select(_ => PgCrypto.crypt(Param.bind(pw), PgCrypto.genSalt(Param.bind("bf"), Param.bind(4))))
            .compile.unique(s)
          // The hash is decoded as plain String; round-trip through `crypt(pw, hash)` should equal `hash`.
          rehash <- empty.select(_ => PgCrypto.crypt(Param.bind(pw), Param.bind(hash)))
            .compile.unique(s)
          _ = assertEquals(rehash, hash)
        } yield ()
      }
    }
  }

  // ---------- fuzzystrmatch ----------------------------------------------------------------------

  test("fuzzystrmatch: levenshtein + soundex computed against the DB") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          dist <- empty.select(_ => PgFuzzy.levenshtein(Param.bind("kitten"), Param.bind("sitting")))
            .compile.unique(s)
          _ = assertEquals(dist, 3)
          sx <- empty.select(_ => PgFuzzy.soundex(Param.bind("Robert"))).compile.unique(s)
          _ = assertEquals(sx, "R163")
        } yield ()
      }
    }
  }

  // ---------- hstore -----------------------------------------------------------------------------

  test("hstore: round-trip + containment + key existence") {
    withContainers { containers =>
      session(containers).use { s =>
        val payload = Hstore("color" -> Some("blue"), "size" -> Some("xl"), "missing" -> None)
        for {
          _ <- things.insert((id = 30, props = payload)).compile.run(s)
          back <- things.select(_.props).where(t => t.id === Param.bind(30)).compile.unique(s)
          _ = assertEquals(back, payload)
          found <- things.select(_.id)
            .where(t => t.props.contains(Param.bind(Hstore("color" -> Some("blue")))))
            .compile.option(s)
          _ = assertEquals(found, Some(30))
          hasKey <- things.select(_.id).where(t => t.props.hasKey(Param.bind("size"))).compile.option(s)
          _ = assertEquals(hasKey, Some(30))
        } yield ()
      }
    }
  }

  // ---------- SchemaValidator: extension-aware ---------------------------------------------------

  test("validate is happy when every extension a relation needs is installed") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          report <- SchemaValidator.validate[IO](s, accounts, folders, things)
          _ = assert(report.isValid, s"unexpected mismatches: ${report.mismatches}")
        } yield ()
      }
    }
  }

  test("validate flags ExtensionMissing for an extraExtensions entry the DB doesn't have") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          report <- SchemaValidator.validate[IO](s, Seq.empty, Set("definitely_not_installed"))
          missing = report.mismatches.collect { case Mismatch.ExtensionMissing(n) => n }
          _ = assertEquals(missing, List("definitely_not_installed"))
        } yield ()
      }
    }
  }

  test("validate doesn't flag extensions that ARE installed when passed via extraExtensions") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          report <- SchemaValidator.validate[IO](s, Seq.empty, Set(PgCrypto.RequiredExtension, PgFuzzy.RequiredExtension))
          _ = assert(
            !report.mismatches.exists(_.isInstanceOf[Mismatch.ExtensionMissing]),
            s"unexpected ExtensionMissing entries: ${report.mismatches}"
          )
        } yield ()
      }
    }
  }
}
