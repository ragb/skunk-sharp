package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object TupleSetParamsSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

/**
 * Tuple-form SET with `Param`s, executed against Postgres from outside the `dsl` package: the Params are typed Args of
 * the compiled template and bind at execute time. (They used to type as `Void` and fail when encoding.)
 */
class TupleSetParamsSuite extends PgFixture {
  import TupleSetParamsSuite.*

  private val users = Table.of[User]("users").withPrimary("id").withDefault("created_at").withDefault("deleted_at")

  // Compiled once, like a repository would.
  private val rename: CommandTemplate[(String, Int, UUID)] =
    users.update.set(u => (u.email := Param[String], u.age := Param[Int])).where(u => u.id === Param[UUID]).compile

  private val upsert: CommandTemplate[(UUID, String, Int, String)] =
    users.insert
      .withParams((id = Param[UUID], email = Param[String], age = Param[Int]))
      .onConflict(u => u.id)
      .doUpdateFromExcluded((u, ex) => (u.age := ex.age, u.email := Param[String]))
      .compile

  test("tuple SET and tuple ON CONFLICT DO UPDATE bind their Params at execute time") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.randomUUID
        for {
          _  <- upsert.run(s)((id, "first@x", 20, "unused@x"))     // plain insert
          _  <- rename.run(s)(("renamed@x", 21, id))
          r1 <- users.select(u => (u.email, u.age)).where(u => u.id === Param.bind(id)).compile.unique(s)
          _  <- upsert.run(s)((id, "ignored@x", 22, "conflict@x")) // conflict → DO UPDATE with the 4th Param
          r2 <- users.select(u => (u.email, u.age)).where(u => u.id === Param.bind(id)).compile.unique(s)
          _ = assertEquals(r1, ("renamed@x", 21))
          _ = assertEquals(r2, ("conflict@x", 22))
        } yield ()
      }
    }
  }
}
