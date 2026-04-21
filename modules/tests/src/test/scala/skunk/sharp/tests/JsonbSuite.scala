package skunk.sharp.tests

import io.circe.Json as CirceJson
import skunk.sharp.dsl.*
import skunk.sharp.circe.*

import java.util.UUID

object JsonbSuite {
  case class Document(id: UUID, body: Jsonb[CirceJson])
}

class JsonbSuite extends PgFixture {
  import JsonbSuite.*

  private val docs = Table.of[Document]("documents")

  test("round-trip: insert jsonb, read back via -> / ->>") {
    withContainers { containers =>
      session(containers).use { s =>
        val id   = UUID.randomUUID
        val body = Jsonb(CirceJson.obj(
          "name"   -> CirceJson.fromString("alice"),
          "age"    -> CirceJson.fromInt(30),
          "active" -> CirceJson.fromBoolean(true)
        ))
        for {
          _ <- docs.insert((id = id, body = body)).compile.run(s)
          // get field as jsonb (wrapped in a Jsonb value) and as text
          row <- docs
            .select(d => (d.body.get("name"), d.body.getText("name"), d.body.getText("age")))
            .where(d => d.id === id)
            .compile.unique(s)
          (nameAsJson, nameAsText, ageAsText) = row
          _                                   = assertEquals(nameAsJson.asString, Some("alice"))
          _                                   = assertEquals(nameAsText, "alice")
          _                                   = assertEquals(ageAsText, "30")
        } yield ()
      }
    }
  }

  test("filter by containment (@>) — find docs whose body contains { status: \"active\" }") {
    withContainers { containers =>
      session(containers).use { s =>
        val matches  = UUID.randomUUID
        val misses   = UUID.randomUUID
        val active   = CirceJson.obj("status" -> CirceJson.fromString("active"), "n" -> CirceJson.fromInt(1))
        val inactive = CirceJson.obj("status" -> CirceJson.fromString("inactive"), "n" -> CirceJson.fromInt(2))
        val probe    = CirceJson.obj("status" -> CirceJson.fromString("active"))
        for {
          _ <- docs.insert.values(
            (id = matches, body = Jsonb(active)),
            (id = misses, body = Jsonb(inactive))
          ).compile.run(s)
          _ <- assertIO(
            docs
              .select(d => d.id)
              .where(d => d.body.contains(param(Jsonb(probe))))
              .compile.run(s).map(_.toSet),
            Set(matches)
          )
        } yield ()
      }
    }
  }

  test("hasKey / hasAnyKey / hasAllKeys") {
    withContainers { containers =>
      session(containers).use { s =>
        val a            = UUID.randomUUID
        val b            = UUID.randomUUID
        val withEmail    = CirceJson.obj("email" -> CirceJson.fromString("a@x"), "tag" -> CirceJson.fromString("t"))
        val withoutEmail = CirceJson.obj("phone" -> CirceJson.fromString("555"))
        for {
          _ <- docs.insert.values(
            (id = a, body = Jsonb(withEmail)),
            (id = b, body = Jsonb(withoutEmail))
          ).compile.run(s)
          _ <- assertIO(docs.select(d => d.id).where(d => d.body.hasKey("email")).compile.run(s).map(_.toSet), Set(a))
          _ <- assertIO(
            docs.select(d => d.id).where(d => d.body.hasAnyKey("email", "phone")).compile.run(s).map(_.toSet),
            Set(a, b)
          )
          _ <- assertIO(
            docs.select(d => d.id).where(d => d.body.hasAllKeys("email", "tag")).compile.run(s).map(_.toSet),
            Set(a)
          )
        } yield ()
      }
    }
  }

  test("typed jsonb — round-trip a case class directly (no io.circe.Json in sight)") {
    import io.circe.{Codec => CirceCodec}

    final case class Preferences(theme: String, notifications: Boolean, tags: List[String])
        derives CirceCodec.AsObject

    final case class TypedDoc(id: UUID, body: Jsonb[Preferences])

    val typedDocs = Table.of[TypedDoc]("documents")
    withContainers { containers =>
      session(containers).use { s =>
        val id    = UUID.randomUUID
        val prefs = Preferences(theme = "dark", notifications = true, tags = List("a", "b"))
        for {
          _   <- typedDocs.insert((id = id, body = Jsonb(prefs))).compile.run(s)
          row <- typedDocs
            .select(d => d.body)
            .where(d => d.id === id)
            .compile.unique(s)
          decoded: Preferences = row // compiles — no .asInstanceOf, just the opaque-type unwrap
          _                    = assertEquals(decoded, prefs)
        } yield ()
      }
    }
  }

  test("typed jsonb + operators — query with .getText / .hasKey / .contains over a Jsonb[CaseClass] column") {
    import io.circe.{Codec => CirceCodec, Json => Cj}

    final case class Profile(name: String, age: Int, tags: List[String])
        derives CirceCodec.AsObject

    final case class PDoc(id: UUID, body: Jsonb[Profile])

    val pdocs = Table.of[PDoc]("documents")
    withContainers { containers =>
      session(containers).use { s =>
        val alice = Profile("jb-alice", 30, List("admin"))
        val bob   = Profile("jb-bob", 25, List("user"))
        val idA   = UUID.randomUUID
        val idB   = UUID.randomUUID
        for {
          _ <- pdocs.insert.values(
            (id = idA, body = Jsonb(alice)),
            (id = idB, body = Jsonb(bob))
          ).compile.run(s)

          // Filter via .getText — jsonb ->> 'name' = 'alice'. The IO returns `List[Jsonb[Profile]]`; opaque-type
          // direction (`Jsonb[A] <: A`) means we need to widen explicitly to match the expected `List[Profile]`.
          _ <- assertIO(
            pdocs
              .select(d => d.body)
              .where(d => d.body.getText("name") === "jb-alice")
              .compile.run(s)
              .map(xs => xs: List[Profile]),
            List(alice)
          )

          // Filter via .contains — jsonb @> '{"name":"jb-alice"}'::jsonb.
          _ <- assertIO(
            pdocs
              .select(d => d.id)
              .where(d => d.body.contains(param(Jsonb[Cj](Cj.obj("name" -> Cj.fromString("jb-alice"))))))
              .compile.run(s),
            List(idA)
          )

          // Project .getText as a String alongside .hasKey.
          _ <- assertIO(
            pdocs
              .select(d => (d.id, d.body.getText("name"), d.body.hasKey("tags")))
              .where(d => d.id === idA)
              .compile.unique(s),
            (idA, "jb-alice", true)
          )
        } yield ()
      }
    }
  }

  test("jsonb_set + jsonb_typeof via the Jsonb namespace") {
    withContainers { containers =>
      session(containers).use { s =>
        val id   = UUID.randomUUID
        val body = Jsonb(CirceJson.obj("version" -> CirceJson.fromInt(1)))
        for {
          _ <- docs.insert((id = id, body = body)).compile.run(s)
          // jsonb_set: bump "version" to 2
          (patched, kind) <- docs
            .select(d =>
              (Jsonb.jsonbSet(d.body, Seq("version"), param(Jsonb(CirceJson.fromInt(2)))), Jsonb.jsonbTypeof(d.body))
            )
            .where(d => d.id === id)
            .compile.unique(s)
          _ = assertEquals(patched.asObject.flatMap(_("version")).flatMap(_.asNumber).flatMap(_.toInt), Some(2))
          _ = assertEquals(kind, "object")
        } yield ()
      }
    }
  }
}
