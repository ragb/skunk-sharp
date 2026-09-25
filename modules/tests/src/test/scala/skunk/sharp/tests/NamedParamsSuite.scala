package skunk.sharp.tests

import cats.effect.IO
import cats.syntax.all.*
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object NamedParamsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Named parameters against Postgres through every execute method. Statements are compiled once; each value is bound by
 * name, and a name used twice is passed once.
 */
class NamedParamsSuite extends PgFixture {
  import NamedParamsSuite.*

  private val users = Table.of[User]("users").withPrimary("id").withDefault("created_at").withDefault("deleted_at")

  // `minAge` appears twice in the WHERE clause — passed once.
  private val byAgeBand =
    users
      .select(u => (u.email, u.age))
      .where(u =>
        u.age >= Param.named["minAge", Int] && u.age <= Param.named["maxAge", Int] &&
          (u.age !== Param.named["minAge", Int]).or(u.email.like(Param.named["pattern", String]))
      )
      .orderBy(u => u.age.asc)
      .compile

  private val insertUser =
    users.insert.withParams((
      id = Param.named["id", UUID],
      email = Param.named["email", String],
      age = Param.named["age", Int]
    )).compile

  private val renameUser =
    users.update.set(u => u.email := Param.named["email", String]).where(u => u.id === Param.named["id", UUID]).compile

  test("named statements run through run / unique / option / stream / cursor / prepared") {
    withContainers { containers =>
      session(containers).use { s =>
        val ids = List.fill(4)(UUID.randomUUID)
        for {
          _ <- ids.zipWithIndex.traverse_ { case (id, i) =>
            insertUser.run(s)((id = id, email = s"n$i@named.test", age = 90 + i))
          }
          all <- byAgeBand.run(s)((minAge = 90, maxAge = 93, pattern = "%@named.test"))
          _ = assertEquals(all.map(_._2), List(90, 91, 92, 93))
          // minAge excluded unless its email matches the pattern
          some <- byAgeBand.run(s)((minAge = 90, maxAge = 93, pattern = "nomatch"))
          _ = assertEquals(some.map(_._2), List(91, 92, 93))
          one <- byAgeBand.unique(s)((minAge = 92, maxAge = 92, pattern = "n2@named.test"))
          _ = assertEquals(one, ("n2@named.test", 92))
          none <- byAgeBand.option(s)((minAge = 200, maxAge = 300, pattern = "%"))
          _ = assertEquals(none, None)
          streamed <- byAgeBand.stream(s, 2)((minAge = 90, maxAge = 93, pattern = "%")).compile.toList
          _ = assertEquals(streamed.size, 4)
          fetched <- byAgeBand.cursor(s)((minAge = 90, maxAge = 93, pattern = "%")).use(_.fetch(10).map(_._1))
          _ = assertEquals(fetched.size, 4)
          prepared <- byAgeBand.prepared(s)
          viaPrep  <- prepared.stream((minAge = 91, maxAge = 92, pattern = "%"), 8).compile.toList
          _ = assertEquals(viaPrep.map(_._2), List(91, 92))
          prepCmd <- renameUser.prepared(s)
          _       <- prepCmd.execute((email = "renamed@named.test", id = ids.head))
          renamed <- users.select(u => u.email).where(u => u.id === Param.bind(ids.head)).compile.unique(s)
          _ = assertEquals(renamed, "renamed@named.test")
        } yield ()
      }
    }
  }
}
