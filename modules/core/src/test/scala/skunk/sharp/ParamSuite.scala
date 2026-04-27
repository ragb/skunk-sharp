package skunk.sharp

import skunk.data.Arr
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.pg.ArrayOps.*
import skunk.sharp.pg.RangeOps.*
import skunk.sharp.pg.tags.PgRange
import skunk.Void

import java.time.LocalDate
import java.util.UUID

object ParamSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String)
  case class Booking(id: UUID, period: PgRange[LocalDate])
  case class Tagged(id: UUID, tags: Arr[String])
}

class ParamSuite extends munit.FunSuite {
  import ParamSuite.{User, Post, Booking, Tagged}

  private val users    = Table.of[User]("users")
  private val posts    = Table.of[Post]("posts")
  private val bookings = Table.of[Booking]("bookings")
  private val tagged   = Table.of[Tagged]("tagged")

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

  // -------- UPDATE SET with Param[T] ------------------------------------------

  test("UPDATE SET: single Param[T] threads typed Args through SET, no WHERE") {
    val cmd = users.update.set(u => u.email := Param[String]).updateAll.compile
    val _: CommandTemplate[String] = cmd
    assertEquals(cmd.fragment.sql.trim, """UPDATE "users" SET "email" = $1""")
  }

  test("UPDATE SET + WHERE: SET Param + WHERE Param compose to (SetT, WhereT)") {
    val cmd = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .compile
    val _: CommandTemplate[(String, UUID)] = cmd
    assert(cmd.fragment.sql.contains(""""email" = $1"""), cmd.fragment.sql)
    assert(cmd.fragment.sql.contains(""""id" = $2"""), cmd.fragment.sql)
  }

  test("UPDATE SET: encoder binds (set, where) values in SQL render order") {
    val cmd = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .compile
    val uid     = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    val af      = cmd.bind(("new@x", uid))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("new@x", uid.toString))
  }

  test("UPDATE SET baked + WHERE Param: SetArgs collapses to Void, Args = WHERE only") {
    val cmd = users.update.set(u => u.email := "fixed").where(u => u.id === Param[UUID]).compile
    val _: CommandTemplate[UUID] = cmd
    assert(cmd.fragment.sql.contains(""""id" = $2"""), cmd.fragment.sql)
  }

  test("UPDATE SET Param + WHERE baked: SetArgs preserved, WArgs = Void") {
    val cmd = users.update.set(u => u.age := Param[Int]).where(u => u.email === "x@y").compile
    val _: CommandTemplate[Int] = cmd
  }

  test("UPDATE SET via & combinator: typed Args flows across both assignments") {
    val cmd = users.update
      .set(u => (u.email := Param[String]) & (u.age := Param[Int]))
      .updateAll
      .compile
    val _: CommandTemplate[(String, Int)] = cmd
  }

  test("UPDATE SET tuple form: SetArgs widens to Void (use & for typed-Args)") {
    val cmd = users.update.set(u => (u.email := "x", u.age := 30)).updateAll.compile
    val _: CommandTemplate[Void] = cmd
  }

  // -------- Param[T] in binary functions / operators / range / array ----------
  //
  // Threading typed Args through projection-position expressions is a separate roadmap item — projections
  // currently bind their fragments at Void. These tests exercise binary builders in WHERE / HAVING / ON
  // positions where Args threads end-to-end through `where(_ => Where[A])`.

  test("Pg.mod with Param in WHERE: comparing a column to mod(Param, Param) threads (Int, Int)") {
    val q = users.select(_.id).where(u => u.age === Pg.mod(Param[Int], Param[Int])).compile
    val _: QueryTemplate[(Int, Int), ?] = q
    assert(q.fragment.sql.contains("mod("), q.fragment.sql)
  }

  test("Pg.power with Param in WHERE: power(col, Param) compared to Param[Double] threads (Double, Double)") {
    val q = users.select(_.id).where(u => Pg.power(u.age, Param[Double]) === Param[Double]).compile
    val _: QueryTemplate[(Double, Double), ?] = q
  }

  test("RangeOps.contains: col @> Param[PgRange[LocalDate]] — Args = PgRange[LocalDate]") {
    val q = bookings.select(_.id).where(b => b.period.contains(Param[PgRange[LocalDate]])).compile
    val _: QueryTemplate[PgRange[LocalDate], ?] = q
    assert(q.fragment.sql.contains("@>"), q.fragment.sql)
  }

  test("ArrayOps.contains: arrCol @> Param[Arr[String]] — Args = Arr[String]") {
    val q = tagged.select(_.id).where(t => t.tags.contains(Param[Arr[String]])).compile
    val _: QueryTemplate[Arr[String], ?] = q
    assert(q.fragment.sql.contains("@>"), q.fragment.sql)
  }

  test("ArrayOps.elemOf: Param[String] = ANY(arrCol) — Args = String") {
    val q = tagged.select(_.id).where(t => Param[String].elemOf(t.tags)).compile
    val _: QueryTemplate[String, ?] = q
    assert(q.fragment.sql.contains("ANY("), q.fragment.sql)
  }

  test("Encoder for Pg.mod(Param, Param) in WHERE supplies both values at execute") {
    val q       = users.select(_.id).where(u => u.age === Pg.mod(Param[Int], Param[Int])).compile
    val af      = q.bind((10, 3))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("10", "3"))
  }

  // -------- Param[T] in single-expression RETURNING ---------------------------

  test("UPDATE … RETURNING column (Void RetArgs) collapses RetArgs cleanly") {
    val q = users.update
      .set(u => u.email := "fixed")
      .where(u => u.id === Param[UUID])
      .returning(u => u.email)
    val _: QueryTemplate[UUID, String] = q
  }

  test("UPDATE SET Param + WHERE Param + RETURNING Param: Args = ((SetT, WhereT), RetT)") {
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returning(u => Pg.power(u.age, Param[Double]))
    val _: QueryTemplate[((String, UUID), Double), Double] = q
  }

  test("DELETE … RETURNING Param-bearing expr threads RetArgs after WArgs") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returning(u => Pg.power(u.age, Param[Double]))
    val _: QueryTemplate[(UUID, Double), Double] = q
  }

  test("INSERT.withParams(...).returning(Param-bearing expr) threads (rowArgs, retArgs)") {
    val q = users.insert
      .withParams((id = Param[UUID], email = Param[String], age = Param[Int]))
      .returning(u => Pg.power(u.age, Param[Double]))
    val _: QueryTemplate[((UUID, String, Int), Double), Double] = q
  }

  test("Encoder for UPDATE Param + WHERE Param + RETURNING Param binds in render order") {
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returning(u => Pg.power(u.age, Param[Double]))
    val uid     = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    val af      = q.bind((("set@x", uid), 2.0))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("set@x", uid.toString, "2.0"))
  }

}
