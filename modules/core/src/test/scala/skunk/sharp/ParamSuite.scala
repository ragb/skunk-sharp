package skunk.sharp

import skunk.sharp.dsl.*
import skunk.Void

import java.util.UUID

object ParamSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String)
}

class ParamSuite extends munit.FunSuite {
  import ParamSuite.{User, Post}

  private val users = Table.of[User]("users")
  private val posts = Table.of[Post]("posts")

  // -------- SELECT WHERE -------------------------------------------------------

  test("SELECT WHERE: single Param[T] yields QueryTemplate[T, R]") {
    val q = users.select.where(u => u.id === Param[UUID]).compile
    val _: QueryTemplate[UUID, ?] = q
    assertEquals(q.fragment.sql.trim, """SELECT "id", "email", "age" FROM "users" WHERE "id" = $1""")
  }

  test("SELECT WHERE: two Params chained via .where compose to (T1, T2)") {
    val q = users.select
      .where(u => u.id === Param[UUID])
      .where(u => u.age >= Param[Int])
      .compile
    val _: QueryTemplate[(UUID, Int), ?] = q
    assertEquals(
      q.fragment.sql.trim,
      """SELECT "id", "email", "age" FROM "users" WHERE ("id" = $1 AND "age" >= $2)"""
    )
  }

  test("SELECT WHERE: Param mixed with && in one lambda composes via Concat2") {
    val q = users.select
      .where(u => u.age >= Param[Int] && u.email === Param[String])
      .compile
    val _: QueryTemplate[(Int, String), ?] = q
    assertEquals(
      q.fragment.sql.trim,
      """SELECT "id", "email", "age" FROM "users" WHERE ("age" >= $1 AND "email" = $2)"""
    )
  }

  test("SELECT WHERE: Param mixed with Param.bind value — typed Args includes only the Param slot") {
    val emailLike = "%@example.com"
    val q = users.select
      .where(u => u.id === Param[UUID])
      .where(u => u.email.like(emailLike))
      .compile
    val _: QueryTemplate[UUID, ?] = q
    val af = q.bind(UUID.fromString("22222222-2222-2222-2222-222222222222"))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded.length, 2, s"expected both bind slots populated; got: $encoded")
  }

  // -------- SELECT HAVING ------------------------------------------------------

  test("SELECT HAVING: Param[T] threads into HArgs") {
    val q = users.select(u => (u.email, Pg.count(u.id)))
      .groupBy(u => u.email)
      .having(_ => Pg.countAll >= Param[Long])
      .compile
    val _: QueryTemplate[Long, ?] = q
    assert(q.fragment.sql.contains("HAVING count(*) >= $1"), q.fragment.sql)
  }

  test("SELECT WHERE+HAVING with Params: typed tuple combines both") {
    val q = users.select(u => (u.email, Pg.count(u.id)))
      .where(u => u.age >= Param[Int])
      .groupBy(u => u.email)
      .having(_ => Pg.countAll >= Param[Long])
      .compile
    val _: QueryTemplate[(Int, Long), ?] = q
  }

  // -------- UPDATE WHERE -------------------------------------------------------

  test("UPDATE WHERE: Param[T] threads into Args") {
    val cmd = users.update
      .set(u => u.email := "fixed@x")
      .where(u => u.id === Param[UUID])
      .compile
    val _: CommandTemplate[UUID] = cmd
    assert(cmd.fragment.sql.contains(""""id" = $2"""), cmd.fragment.sql)
  }

  // -------- DELETE WHERE -------------------------------------------------------

  test("DELETE WHERE: Param[T] threads into Args") {
    val cmd = users.delete.where(u => u.id === Param[UUID]).compile
    val _: CommandTemplate[UUID] = cmd
    assertEquals(cmd.fragment.sql.trim, """DELETE FROM "users" WHERE "id" = $1""")
  }

  test("DELETE WHERE: chained Params compose to (T1, T2)") {
    val cmd = users.delete
      .where(u => u.id === Param[UUID])
      .where(u => u.age <= Param[Int])
      .compile
    val _: CommandTemplate[(UUID, Int)] = cmd
  }

  // -------- DELETE … RETURNING ------------------------------------------------

  test("DELETE … RETURNING with Param[T]: typed Args preserved across RETURNING") {
    val q = users.delete.where(u => u.id === Param[UUID]).returning(u => u.email)
    val _: QueryTemplate[UUID, String] = q
  }

  // -------- JOIN ---------------------------------------------------------------

  test("JOIN with Param[T] in WHERE: typed Args threads") {
    val q = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .where(r => r.users.id === Param[UUID])
      .compile
    val _: QueryTemplate[UUID, ?] = q
    assert(q.fragment.sql.contains(""""users"."id" = $1"""), q.fragment.sql)
  }

  // -------- bind / execute path ------------------------------------------------

  test("Encoder produces bound values when invoked with the supplied Param value") {
    val q   = users.select.where(u => u.id === Param[UUID]).compile
    val uid = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val af  = q.bind(uid)
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List(uid.toString))
  }

  test("Encoder for two-Param query produces both values from the tuple") {
    val q = users.select
      .where(u => u.id === Param[UUID])
      .where(u => u.age >= Param[Int])
      .compile
    val uid = UUID.fromString("33333333-3333-3333-3333-333333333333")
    val af  = q.bind((uid, 21))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List(uid.toString, "21"))
  }

  // -------- Limitations the migration knows about ------------------------------

  test("INSERT values: Args = Void (values bake via Param.bind)") {
    val cmd = users.insert((id = UUID.randomUUID, email = "x@y", age = 30))
    val _: CommandTemplate[Void] = cmd.compile
  }

  test("INSERT.withParams: typed Args = the row tuple") {
    val cmd = users.insert.withParams((id = Param[UUID], email = Param[String], age = Param[Int])).compile
    val _: CommandTemplate[(UUID, String, Int)] = cmd
    assert(cmd.fragment.sql.contains("VALUES"), cmd.fragment.sql)
  }

  test("INSERT.withParams: encoder produces all bound values from the supplied tuple") {
    val cmd = users.insert.withParams((id = Param[UUID], email = Param[String], age = Param[Int])).compile
    val uid = UUID.fromString("55555555-5555-5555-5555-555555555555")
    val af  = cmd.bind((uid, "x@y", 30))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List(uid.toString, "x@y", "30"))
  }

  test("UPDATE SET: SetArgs = Void — use baked values / Param.bind, NOT Param[T]") {
    // KNOWN LIMITATION: `.set(... := Param[T])` is unsafe — `.set` forces SetArgs = Void, dropping the
    // Param's type, and the SET fragment's encoder still expects a T at execute time. Routing Void
    // through it ClassCastException's at the codec. Use baked values (Param.bind path) for SET.
    // Re-add Param[T] in SET when per-row Args reduction lands.
    val cmd = users.update.set(u => u.email := "fixed").where(u => u.id === Param[UUID]).compile
    val _: CommandTemplate[UUID] = cmd
  }

}
