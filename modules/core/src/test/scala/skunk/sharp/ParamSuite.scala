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
  case class Task(id: UUID, title: String, priority: Int, due: LocalDate)
}

class ParamSuite extends munit.FunSuite {
  import ParamSuite.{User, Post, Booking, Tagged, Task}

  private val users    = Table.of[User]("users")
  private val posts    = Table.of[Post]("posts")
  private val bookings = Table.of[Booking]("bookings")
  private val tagged   = Table.of[Tagged]("tagged")
  private val tasks    = Table.of[Task]("tasks").withPrimary("id")

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
      .where(u => u.email.like(Param.bind(emailLike)))
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
      .set(u => u.email := lit("fixed@x"))
      .where(u => u.id === Param[UUID])
      .compile
    val _: CommandTemplate[UUID] = cmd
    assert(cmd.fragment.sql.contains(""""id" = $1"""), cmd.fragment.sql)
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

  test("UPDATE SET literal + WHERE Param: SetArgs collapses to Void, Args = WHERE only") {
    val cmd = users.update.set(u => u.email := lit("fixed")).where(u => u.id === Param[UUID]).compile
    val _: CommandTemplate[UUID] = cmd
    assert(cmd.fragment.sql.contains(""""id" = $1"""), cmd.fragment.sql)
  }

  test("UPDATE SET Param + WHERE literal: SetArgs preserved, WArgs = Void") {
    val cmd = users.update.set(u => u.age := Param[Int]).where(u => u.email === lit("x@y")).compile
    val _: CommandTemplate[Int] = cmd
  }

  test("UPDATE SET via & combinator: typed Args flows across both assignments") {
    val cmd = users.update
      .set(u => (u.email := Param[String]) & (u.age := Param[Int]))
      .updateAll
      .compile
    val _: CommandTemplate[(String, Int)] = cmd
  }

  test("UPDATE SET tuple form with literals: SetArgs widens to Void") {
    val cmd = users.update.set(u => (u.email := lit("x"), u.age := lit(30))).updateAll.compile
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
      .set(u => u.email := lit("fixed"))
      .where(u => u.id === Param[UUID])
      .returning(u => u.email)
    val _: QueryTemplate[UUID, String] = q
  }

  test("UPDATE SET Param + WHERE Param + RETURNING Param: Args flattens to (SetT, WhereT, RetT)") {
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returning(u => Pg.power(u.age, Param[Double]))
    val _: QueryTemplate[(String, UUID, Double), Double] = q
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
    val _: QueryTemplate[(UUID, String, Int, Double), Double] = q
  }

  test("Encoder for UPDATE Param + WHERE Param + RETURNING Param binds in render order") {
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returning(u => Pg.power(u.age, Param[Double]))
    val uid     = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    val af      = q.bind(("set@x", uid, 2.0))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("set@x", uid.toString, "2.0"))
  }

  // -------- Multi-item RETURNING with Param[T] threading ----------------------

  test("DELETE … RETURNING tuple of all-column refs: RetArgs collapses to Void") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (u.id, u.email))
    val _: QueryTemplate[UUID, (UUID, String)] = q
  }

  test("DELETE … RETURNING tuple with one Param: RetArgs threads single") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (u.id, Pg.power(u.age, Param[Double])))
    val _: QueryTemplate[(UUID, Double), (UUID, Double)] = q
  }

  test("DELETE … RETURNING tuple with two Params: RetArgs threads tuple") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (Pg.power(u.age, Param[Double]), Pg.mod(u.age, Param[Int])))
    // RETURNING items combine via Concat — flat-tuple Concat collapses everything into a single
    // user-facing tuple: (UUID from WHERE, Double + Int from RETURNING) = (UUID, Double, Int).
    val _: QueryTemplate[(UUID, Double, Int), (Double, Int)] = q
  }

  test("UPDATE … RETURNING tuple with Params: flat (SetT, WhereT, RetArgs…) shape") {
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (u.id, Pg.power(u.age, Param[Double])))
    val _: QueryTemplate[(String, UUID, Double), (UUID, Double)] = q
  }

  test("INSERT.withParams + returningTuple with Param: row Args + ret Args compose flat") {
    val q = users.insert
      .withParams((id = Param[UUID], email = Param[String], age = Param[Int]))
      .returningTuple(u => (u.id, Pg.power(u.age, Param[Double])))
    val _: QueryTemplate[(UUID, String, Int, Double), (UUID, Double)] = q
  }

  test("returningAll still produces QueryTemplate[WArgs, NamedRow]") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningAll
    val _: QueryTemplate[UUID, ?] = q
  }

  test("Encoder for DELETE … RETURNING (col, Param) binds where + ret values in render order") {
    val q = users.delete
      .where(u => u.age >= Param[Int])
      .returningTuple(u => (u.id, Pg.power(u.age, Param[Double])))
    val af      = q.bind((30, 2.0))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("30", "2.0"))
  }

  test("Encoder for multi-item RETURNING with two Params binds both") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (Pg.power(u.age, Param[Double]), Pg.mod(u.age, Param[Int])))
    val uid     = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
    val af      = q.bind((uid, 2.0, 7))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List(uid.toString, "2.0", "7"))
  }

  test("SQL render for multi-item RETURNING places items comma-separated") {
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningTuple(u => (u.id, u.email))
    assert(q.fragment.sql.contains("""RETURNING "id", "email""""), q.fragment.sql)
  }

  // -------- SELECT projection: typed Args threading --------------------------

  test("SELECT projection: plain tuple of column refs collapses ProjArgs to Void") {
    val q = users.select(u => (u.id, u.email)).compile
    val _: QueryTemplate[Void, ?] = q
  }

  test("SELECT projection: single column ref collapses ProjArgs to Void") {
    val q = users.select(u => u.email).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("SELECT projection: single Param-bearing expr threads ProjArgs") {
    val q = users.select(u => Pg.power(u.age, Param[Double])).compile
    val _: QueryTemplate[Double, Double] = q
  }

  test("SELECT projection: tuple with one Param threads single ProjArgs") {
    val q = users.select(u => (u.id, Pg.power(u.age, Param[Double]))).compile
    val _: QueryTemplate[Double, (UUID, Double)] = q
  }

  test("SELECT projection: tuple with two Params threads (D1, D2)") {
    val q = users
      .select(u => (Pg.power(u.age, Param[Double]), Pg.mod(u.age, Param[Int])))
      .compile
    val _: QueryTemplate[(Double, Int), (Double, Int)] = q
  }

  test("SELECT projection: named tuple (no Param) compiles cleanly") {
    val q = users.select(u => (email = u.email, age = u.age)).compile
    val _: QueryTemplate[Void, ?] = q
    // SQL render is unchanged from plain tuple
    assert(q.fragment.sql.contains(""""email", "age""""), q.fragment.sql)
  }

  test("SELECT projection + WHERE: ProjArgs + WArgs compose") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .where(u => u.id === Param[UUID])
      .compile
    val _: QueryTemplate[(Double, UUID), Double] = q
  }

  test("Encoder for SELECT proj-Param + WHERE-Param binds in render order") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .where(u => u.id === Param[UUID])
      .compile
    val uid     = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd")
    val af      = q.bind((2.5, uid))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("2.5", uid.toString))
  }

  // -------- GROUP BY: typed Args threading -----------------------------------

  test("SELECT … groupBy(non-Param column) collapses GArgs to Void") {
    val q = users.select(u => (u.age, Pg.countAll)).groupBy(u => u.age).compile
    val _: QueryTemplate[Void, ?] = q
  }

  test("SELECT … groupBy(Param-bearing expr) threads GArgs into Args slot") {
    val q = users
      .select(_ => Pg.countAll)
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .compile
    val _: QueryTemplate[Int, Long] = q
  }

  test("SELECT proj-Param + WHERE-Param + GROUP-Param: full 4-slot composition") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .compile
    val _: QueryTemplate[(Double, UUID, Int), Double] = q
  }

  test("Encoder for proj-Param + WHERE-Param + GROUP-Param binds in render order") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .compile
    val uid     = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
    val af      = q.bind((1.5, uid, 7))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("1.5", uid.toString, "7"))
  }

  test("Pre-projection: SelectBuilder.groupBy(Param) → .select threads GArgs through Groups") {
    val q = users
      .select
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .select(_ => Pg.countAll)
      .compile
    val _: QueryTemplate[Int, Long] = q
  }

  test("Pre-projection groupBy(Param) encodes value in render order") {
    val q = users
      .select
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .select(_ => Pg.countAll)
      .compile
    val af      = q.bind(7)
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("7"))
  }

  // -------- DISTINCT ON: typed Args threading --------------------------------

  test("SELECT DISTINCT ON column ref collapses DArgs to Void") {
    val q = users.select(u => u.email).distinctOn(u => u.email).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("SELECT DISTINCT ON Param-bearing expr threads DArgs ahead of PROJ") {
    val q = users
      .select(u => u.email)
      .distinctOn(u => Pg.mod(u.age, Param[Int]))
      .compile
    val _: QueryTemplate[Int, String] = q
  }

  test("DISTINCT-Param + proj-Param + WHERE-Param + GROUP-Param: full 5-slot composition") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .distinctOn(u => Pg.mod(u.age, Param[Int]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .compile
    // Args = Concat[Concat[Concat[Concat[DArgs=Int, ProjArgs=Double], WArgs=UUID], GArgs=Int], HArgs=Void]
    // collapses to flat (Int, Double, UUID, Int) at the user-facing site.
    val _: QueryTemplate[(Int, Double, UUID, Int), Double] = q
  }

  test("Encoder for full 5-slot composition binds in render order (DIST → PROJ → WHERE → GROUP)") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .distinctOn(u => Pg.mod(u.age, Param[Int]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .compile
    val uid     = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    val af      = q.bind((11, 2.0, uid, 7))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("11", "2.0", uid.toString, "7"))
  }

  // -------- ORDER BY: typed Args threading -----------------------------------

  test("SELECT … orderBy(column.desc) collapses OArgs to Void") {
    val q = users.select(u => u.email).orderBy(u => u.age.desc).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("SELECT … orderBy(Param-bearing expr.desc) threads OArgs") {
    val q = users
      .select(u => u.email)
      .orderBy(u => Pg.mod(u.age, Param[Int]).desc)
      .compile
    val _: QueryTemplate[Int, String] = q
  }

  test("Full 6-slot composition: DIST + PROJ + WHERE + GROUP + ORDER all Param-bearing") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .distinctOn(u => Pg.mod(u.age, Param[Int]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .orderBy(u => Pg.mod(u.age, Param[Int]).asc)
      .compile
    // Args (DArgs, ProjArgs, WArgs, GArgs, HArgs=Void, OArgs) collapses to flat user-facing tuple.
    val _: QueryTemplate[(Int, Double, UUID, Int, Int), Double] = q
  }

  test("Encoder for full 6-slot composition binds in render order (DIST→PROJ→WHERE→GROUP→ORDER)") {
    val q = users
      .select(u => Pg.power(u.age, Param[Double]))
      .distinctOn(u => Pg.mod(u.age, Param[Int]))
      .where(u => u.id === Param[UUID])
      .groupBy(u => Pg.mod(u.age, Param[Int]))
      .orderBy(u => Pg.mod(u.age, Param[Int]).asc)
      .compile
    val uid     = UUID.fromString("aaaaaaaa-1111-2222-3333-444444444444")
    val af      = q.bind((11, 2.0, uid, 7, 13))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("11", "2.0", uid.toString, "7", "13"))
  }

  // -------- Variadic-typed: Pg.coalesce / Pg.concat ---------------------------

  test("Pg.coalesce(col) preserves Args = Void for column ref") {
    val q = users.select(u => Pg.coalesce(u.email)).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("Pg.coalesce(col, Param) threads Args = Param's type via Concat") {
    val q = users.select(u => Pg.coalesce(u.email, Param[String])).compile
    val _: QueryTemplate[String, String] = q
  }

  test("Pg.coalesce(Param, col, Param) at arity 3 threads (A1, A3) via left-fold Concat") {
    val q = users.select(u => Pg.coalesce(Param[String], u.email, Param[String])).compile
    // (((String, Void), String) collapsed = (String, String)
    val _: QueryTemplate[(String, String), String] = q
  }

  test("Pg.coalesce(col*5) all column refs reduces Args = Void") {
    val q = users.select(u => Pg.coalesce(u.email, u.email, u.email, u.email, u.email)).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("Pg.coalesce at arity 5 threads typed Args flat") {
    val q = users.select(u => Pg.coalesce(Param[String], u.email, Param[String], u.email, Param[String])).compile
    // FoldConcat over (String, Void, String, Void, String) flattens to (String, String, String).
    val _: QueryTemplate[(String, String, String), String] = q
  }

  test("Pg.greatest at arity 6 threads typed Args flat") {
    val q = users.select(u =>
      Pg.greatest(Param[Int], u.age, Param[Int], u.age, Param[Int], u.age)
    ).compile
    val _: QueryTemplate[(Int, Int, Int), Int] = q
  }

  test("Pg.least at arity 9 threads typed Args flat") {
    val q = users.select(u =>
      Pg.least(Param[Int], u.age, Param[Int], u.age, Param[Int], u.age, Param[Int], u.age, Param[Int])
    ).compile
    // FoldConcat over (Int, Void, Int, Void, Int, Void, Int, Void, Int) flattens to (Int*5).
    val _: QueryTemplate[(Int, Int, Int, Int, Int), Int] = q
  }

  test("Pg.concat(col, Param) threads typed Args") {
    val q = users.select(u => Pg.concat(u.email, Param[String])).compile
    val _: QueryTemplate[String, String] = q
  }

  test("Encoder for Pg.coalesce(Param, col, Param) binds the two Params in render order") {
    val q = users
      .select(u => Pg.coalesce(Param[String], u.email, Param[String]).as("v"))
      .where(u => u.id === Param[UUID])
      .compile
    val uid     = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val af      = q.bind(("first", "third", uid))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("first", "third", uid.toString))
  }

  test("Pg.greatest at arity 2 threads typed Args") {
    val q = users.select(u => Pg.greatest(u.age, Param[Int])).compile
    val _: QueryTemplate[Int, Int] = q
  }

  test("Pg.least at arity 3 threads typed Args via left-fold Concat") {
    val q = users.select(u => Pg.least(Param[Int], u.age, Param[Int])).compile
    val _: QueryTemplate[(Int, Int), Int] = q
  }

  test("Pg.makeDate(Param, lit, Param) threads (Y, D) via Concat") {
    val q = empty.select(_ => Pg.makeDate(Param[Int], lit(1), Param[Int])).compile
    val _: QueryTemplate[(Int, Int), java.time.LocalDate] = q
  }

  test("Pg.overlaps(Param, Param, Param, Param) threads 4-slot Concat") {
    val q = bookings
      .select(b => b.id)
      .where(_ =>
        Pg.overlaps(
          Param[java.time.LocalDate],
          Param[java.time.LocalDate],
          Param[java.time.LocalDate],
          Param[java.time.LocalDate]
        )
      )
      .compile
    val _: QueryTemplate[(java.time.LocalDate, java.time.LocalDate, java.time.LocalDate, java.time.LocalDate), ?] = q
  }

  test("Pg.lpad(col, 4, fill) propagates Args from the typed expr only") {
    val q = users.select(u => Pg.lpad(u.email, 4, "*")).compile
    val _: QueryTemplate[Void, String] = q
  }

  test("Pg.rpad(Param, 4, fill) threads Param through expr position") {
    val q = empty.select(_ => Pg.rpad(Param[String], 4, "_")).compile
    val _: QueryTemplate[String, String] = q
  }

  test("Pg.lag(Param, 1, default) threads Param through expr position") {
    val q = users.select(_ => Pg.lag(Param[String], 1, "n/a")).compile
    val _: QueryTemplate[String, String] = q
  }

  // -------- CASE WHEN: typed Args threading ---------------------------------

  test("CASE WHEN with all-Void branches collapses Args to Void") {
    val q = users
      .select(u => caseWhen(u.age < lit(18), lit("minor")).otherwise(lit("adult")))
      .compile
    val _: QueryTemplate[Void, String] = q
  }

  test("CASE WHEN with Param in cond + ELSE Param threads (Int, String) via Concat") {
    val q = users
      .select(_ =>
        caseWhen(Param[Int] === lit(0), lit("zero"))
          .otherwise(Param[String])
      )
      .compile
    val _: QueryTemplate[(Int, String), String] = q
  }

  test("CASE WHEN with Param in branch result threads Param's type") {
    val q = users
      .select(u =>
        caseWhen(u.age < lit(18), Param[String])
          .otherwise(lit("adult"))
      )
      .compile
    val _: QueryTemplate[String, String] = q
  }

  test("CASE WHEN multi-branch: cond1=Param, branch2=Param, ELSE=Param threads three Args") {
    val q = users
      .select(_ =>
        caseWhen(Param[Int] === lit(1), lit("a"))
          .when(Param[Int] === lit(2), Param[String])
          .otherwise(Param[String])
      )
      .compile
    // ProjArgsOf folds right-to-left over Items + ELSE; Void slots collapse and the flat-tuple Concat
    // produces (Int, Int, String, String) at the user-facing site.
    val _: QueryTemplate[(Int, Int, String, String), String] = q
  }

  test("CASE WHEN .end produces Option[T] with typed Args") {
    val q = users
      .select(_ => caseWhen(Param[Int] === lit(0), lit("zero")).end)
      .compile
    val _: QueryTemplate[Int, Option[String]] = q
  }

  // -------- ON CONFLICT DO UPDATE typed Args --------------------------------------------------

  test("onConflict.doUpdate with Param yields CommandTemplate[CA]") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doUpdate(t => t.title := Param[String])
      .compile
    val _: CommandTemplate[String] = cmd
  }

  test("onConflict.doUpdate with Param produces correct SQL") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doUpdate(t => t.title := Param[String])
      .compile
    assert(
      cmd.fragment.sql.endsWith("""ON CONFLICT ("id") DO UPDATE SET "title" = $5"""),
      s"unexpected sql: ${cmd.fragment.sql}"
    )
  }

  test("onConflict.doUpdate with two Params via & yields CommandTemplate[(String, Int)]") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doUpdate(t => (t.title := Param[String]) & (t.priority := Param[Int]))
      .compile
    val _: CommandTemplate[(String, Int)] = cmd
  }

  test("onConflict.doUpdate tuple overload (literal RHS) yields CommandTemplate[Void]") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doUpdate(t => (t.title := lit("updated"), t.priority := lit(9)))
      .compile
    val _: CommandTemplate[Void] = cmd
  }

  test("onConflict.doUpdateFromExcluded with Param yields CommandTemplate[CA]") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doUpdateFromExcluded((t, _) => t.title := Param[String])
      .compile
    val _: CommandTemplate[String] = cmd
  }

  test("onConflict.doNothing after Param insert yields CommandTemplate[Void]") {
    val cmd = tasks.insert
      .apply((id = UUID.randomUUID(), title = "t", priority = 1, due = LocalDate.now()))
      .onConflict(t => t.id)
      .doNothing
      .compile
    val _: CommandTemplate[Void] = cmd
  }

  // -------- UPDATE SET mixed baked + Param via & -----------------------------------------

  test("UPDATE SET mixed literal + Param yields CommandTemplate[String]") {
    val cmd = users.update
      .set(u => (u.age := lit(0)) & (u.email := Param[String]))
      .updateAll
      .compile
    val _: CommandTemplate[String] = cmd
    assertEquals(cmd.fragment.sql.trim, """UPDATE "users" SET "age" = 0, "email" = $1""")
  }

  // -------- UPDATE … FROM / DELETE … USING typed Args ------------------------------------

  test("UPDATE … FROM Param in WHERE yields CommandTemplate[UUID]") {
    val cmd = users.update
      .from(posts)
      .set(r => r.users.age := lit(0))
      .where(r => r.users.id === Param[UUID])
      .compile
    val _: CommandTemplate[UUID] = cmd
    assertEquals(
      cmd.fragment.sql.trim,
      """UPDATE "users" SET "age" = 0 FROM "posts" WHERE "users"."id" = $1"""
    )
  }

  test("UPDATE … FROM Param in SET yields CommandTemplate[String]") {
    val cmd = users.update
      .from(posts)
      .set(r => r.users.email := Param[String])
      .where(r => r.users.id ==== r.posts.user_id)
      .compile
    val _: CommandTemplate[String] = cmd
    assertEquals(
      cmd.fragment.sql.trim,
      """UPDATE "users" SET "email" = $1 FROM "posts" WHERE "users"."id" = "posts"."user_id""""
    )
  }

  test("UPDATE … FROM Param in SET and WHERE yields CommandTemplate[(String, UUID)]") {
    val cmd = users.update
      .from(posts)
      .set(r => r.users.email := Param[String])
      .where(r => r.users.id === Param[UUID])
      .compile
    val _: CommandTemplate[(String, UUID)] = cmd
    assertEquals(
      cmd.fragment.sql.trim,
      """UPDATE "users" SET "email" = $1 FROM "posts" WHERE "users"."id" = $2"""
    )
  }

  test("DELETE … USING Param in WHERE yields CommandTemplate[UUID]") {
    val cmd = users.delete
      .using(posts)
      .where(r => r.users.id === Param[UUID])
      .compile
    val _: CommandTemplate[UUID] = cmd
    assertEquals(
      cmd.fragment.sql.trim,
      """DELETE FROM "users" USING "posts" WHERE "users"."id" = $1"""
    )
  }

  // -------- Compile-time guards: Param not allowed in subquery / CTE / ON positions ---------------

  test("SelectBuilder.alias threads Param[UUID] from inner WHERE into outer Args") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val sub   = users.select.where(u => u.id === Param[UUID]).alias("u")
    val qt    = sub.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assertEquals(
      qt.fragment.sql.trim,
      """SELECT "id", "email", "age" FROM (SELECT "id", "email", "age" FROM "users" WHERE "id" = $1) AS "u""""
    )
  }

  test("cte WHERE Param threads typed Args into outer compile") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val active = cte("active", users.select.where(u => u.id === Param[UUID]))
    val qt = active.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assert(qt.fragment.sql.startsWith("""WITH "active" AS ("""), qt.fragment.sql)
    assert(qt.fragment.sql.contains("""WHERE "id" = $1"""), qt.fragment.sql)
  }

  test("ProjectedSelect.alias threads Param[UUID] from inner WHERE into outer Args") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val sub   = users.select(u => u.email).where(u => u.id === Param[UUID]).alias("u")
    val qt    = sub.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assertEquals(
      qt.fragment.sql.trim,
      """SELECT "email" FROM (SELECT "email" FROM "users" WHERE "id" = $1) AS "u""""
    )
  }

  test("cte of projected SELECT WHERE Param threads typed Args into outer compile") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val byId  = cte("by_id", users.select(u => u.email.as("e")).where(u => u.id === Param[UUID]))
    val qt    = byId.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assert(qt.fragment.sql.contains("""WITH "by_id" AS ("""), qt.fragment.sql)
    assert(qt.fragment.sql.contains("""WHERE "id" = $1"""), qt.fragment.sql)
  }

  test("typed-args CTE re-aliased via .alias preserves BodyArgs at type level") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val byId  = cte("by_id", users.select.where(u => u.id === Param[UUID]))
    // `.alias("x")` returns a new CteRelation with cteName = "by_id" and aliasName = "x" —
    // BodyArgs (UUID) survives the re-alias and surfaces in the outer Args.
    val aliased = byId.alias("x")
    val qt = aliased.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assert(qt.fragment.sql.contains("""WITH "by_id" AS ("""), qt.fragment.sql)
    assert(qt.fragment.sql.contains(""""by_id" AS "x""""), qt.fragment.sql)
    assert(qt.fragment.sql.contains("""WHERE "id" = $1"""), qt.fragment.sql)
  }

  // -------- Typed JOIN ON Args threading ---------------------------------------------------------

  test("JOIN ON Param threads typed Args into outer compile") {
    case class User(id: UUID, email: String)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val qt = users
      .innerJoin(posts)
      .on(r => (r.users.id ==== r.posts.user_id) && (r.posts.id === Param[UUID]))
      .select(r => (r.users.email, r.posts.title))
      .compile
    val _: QueryTemplate[UUID, (String, String)] = qt
    assert(qt.fragment.sql.contains("""ON ("""), qt.fragment.sql)
    assert(qt.fragment.sql.contains("""$1"""), qt.fragment.sql)
  }

  test("JOIN ON Param + outer WHERE Param accumulates in render order") {
    case class User(id: UUID, email: String, age: Int)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val qt = users
      .innerJoin(posts)
      .on(r => (r.users.id ==== r.posts.user_id) && (r.posts.title === Param[String]))
      .select(r => (r.users.email, r.posts.title))
      .where(r => r.users.age >= Param[Int])
      .compile
    val _: QueryTemplate[(String, Int), (String, String)] = qt
    assert(qt.fragment.sql.contains("""$1"""), qt.fragment.sql)  // ON pred Param
    assert(qt.fragment.sql.contains("""$2"""), qt.fragment.sql)  // WHERE Param
  }

  test("JOIN ON column-only (Void) collapses cleanly: outer Args = WHERE Args only") {
    case class User(id: UUID, email: String, age: Int)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val qt = users
      .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .where(r => r.users.age >= Param[Int])
      .compile
    val _: QueryTemplate[Int, (String, String)] = qt
  }

  // -------- Multi-source typed-alias Args threading -----------------------------------------------

  test("typed alias on join tail thread Param[UUID] into outer Args") {
    case class User(id: UUID, email: String)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val byId  = posts.select.where(p => p.id === Param[UUID]).alias("p")
    val qt = users
      .innerJoin(byId)
      .on(r => r.users.id ==== r.p.user_id)
      .select(r => (r.users.email, r.p.title))
      .compile
    val _: QueryTemplate[UUID, (String, String)] = qt
    assertEquals(
      qt.fragment.sql.trim,
      """SELECT "users"."email", "p"."title" FROM "users" INNER JOIN (SELECT "id", "user_id", "title" FROM "posts" WHERE "id" = $1) AS "p" ON "users"."id" = "p"."user_id""""
    )
  }

  test("typed alias on both head and tail accumulates Params in render order") {
    case class User(id: UUID, email: String)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users  = Table.of[User]("users")
    val posts  = Table.of[Post]("posts")
    val byMail = users.select.where(u => u.email === Param[String]).alias("u")
    val byId   = posts.select.where(p => p.id === Param[UUID]).alias("p")
    val qt = byMail
      .innerJoin(byId)
      .on(r => r.u.id ==== r.p.user_id)
      .select(r => (r.u.email, r.p.title))
      .compile
    val _: QueryTemplate[(String, UUID), (String, String)] = qt
    assert(qt.fragment.sql.contains("""(SELECT "id", "email" FROM "users" WHERE "email" = $1) AS "u""""), qt.fragment.sql)
    assert(qt.fragment.sql.contains("""(SELECT "id", "user_id", "title" FROM "posts" WHERE "id" = $2) AS "p""""), qt.fragment.sql)
  }

  test("typed alias inside outer WHERE Param accumulates in render order [SArgs, WArgs]") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val sub   = users.select.where(u => u.id === Param[UUID]).alias("u")
    val qt    = sub.select.where(u => u.email === Param[String]).compile
    val _: QueryTemplate[(UUID, String), ?] = qt
    assert(qt.fragment.sql.contains("WHERE \"id\" = $1"), qt.fragment.sql)
    assert(qt.fragment.sql.contains("WHERE \"email\" = $2"), qt.fragment.sql)
  }

  test("nested typed aliases — inner Param surfaces through both alias layers") {
    case class User(id: UUID, email: String)
    val users  = Table.of[User]("users")
    val inner  = users.select.where(u => u.id === Param[UUID]).alias("inner")
    val outer  = inner.select.alias("outer")
    val qt     = outer.select.compile
    val _: QueryTemplate[UUID, ?] = qt
    assertEquals(
      qt.fragment.sql.trim,
      """SELECT "id", "email" FROM (SELECT "id", "email" FROM (SELECT "id", "email" FROM "users" WHERE "id" = $1) AS "inner") AS "outer""""
    )
  }

  test("typed projected alias as join tail — Param surfaces via outer compile") {
    case class User(id: UUID, email: String)
    case class Post(id: UUID, user_id: UUID, title: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val sub   = posts.select(p => (p.user_id, p.title)).where(p => p.id === Param[UUID]).alias("p")
    val qt = users
      .innerJoin(sub)
      .on(r => r.users.id ==== r.p.user_id)
      .select(r => (r.users.email, r.p.title))
      .compile
    val _: QueryTemplate[UUID, (String, String)] = qt
    assert(qt.fragment.sql.contains("""(SELECT "user_id", "title" FROM "posts" WHERE "id" = $1) AS "p""""), qt.fragment.sql)
  }

  // -------- Named-tuple RETURNING typed Args -----------------------------------------------------

  test("DELETE.returningNamed threads typed Args + projects to NamedTuple row") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users.delete
      .where(u => u.id === Param[UUID])
      .returningNamed(u => (id = u.id, email = u.email))
    type Row = (id: UUID, email: String)
    val _: QueryTemplate[UUID, Row] = q
    assert(q.fragment.sql.endsWith("""RETURNING "id", "email""""), q.fragment.sql)
  }

  test("INSERT.returningNamed with Param-bearing items threads (rowArgs, retArgs) and labels") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val uid   = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val q = users
      .insert((id = uid, email = "a@b", age = 30))
      .returningNamed(u => (id = u.id, p = Pg.power(u.age, Param[Double])))
    type Row = (id: UUID, p: Double)
    val _: QueryTemplate[Double, Row] = q
  }

  test("UPDATE.returningNamed with multiple Params + named labels") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users.update
      .set(u => u.email := Param[String])
      .where(u => u.id === Param[UUID])
      .returningNamed(u => (powered = Pg.power(u.age, Param[Double]), modded = Pg.mod(u.age, Param[Int])))
    type Row = (powered: Double, modded: Int)
    val _: QueryTemplate[(String, UUID, Double, Int), Row] = q
  }

  // -------- IN / ANY / ALL subquery typed Args ---------------------------------------------------

  test("col.in(<Param-bearing subquery>) threads inner Args via Concat") {
    case class User(id: UUID, email: String, age: Int)
    case class Post(id: UUID, user_id: UUID, status: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val activeUsers = posts.select(p => p.user_id).where(p => p.status === Param[String])
    val q = users.select.where(u => u.id.in(activeUsers)).compile
    val _: QueryTemplate[String, ?] = q
    assert(q.fragment.sql.contains("\"status\" = $1"), q.fragment.sql)
  }

  test("col.lteAny(<Param-bearing subquery>) threads inner Args via Concat") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val ages  = users.select(u => u.age).where(u => u.email === Param[String])
    val q     = users.select.where(u => u.age.lteAny(ages)).compile
    val _: QueryTemplate[String, ?] = q
    assert(q.fragment.sql.contains("<= ANY") && q.fragment.sql.contains("$1"), q.fragment.sql)
  }

  test("col.in(values) preserves Args = Void (values are Param.bind-baked)") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q     = users.select.where(u => u.age.in(cats.data.NonEmptyList.of(20, 21, 22))).compile
    val _: QueryTemplate[Void, ?] = q
  }

  // -------- UPDATE FROM / DELETE USING with typed-args FROM tail --------------------------------

  test("UPDATE … FROM <typed-args subquery> threads inner Param into outer Args") {
    case class User(id: UUID, email: String, age: Int)
    case class Post(id: UUID, user_id: UUID, status: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val activePosts = posts.select.where(p => p.status === Param[String]).alias("ap")
    val cmd = users.update
      .from(activePosts)
      .set(r => r.users.age := lit(0))
      .where(r => r.users.id ==== r.ap.user_id)
      .compile
    val _: CommandTemplate[String] = cmd
    assert(cmd.fragment.sql.contains("""FROM (SELECT "id", "user_id", "status" FROM "posts" WHERE "status" = $1) AS "ap""""), cmd.fragment.sql)
  }

  // -------- Window OVER (…) typed Args ----------------------------------------------------------

  test("WindowSpec.partitionBy(Param) threads typed Args via Concat") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users.select(_ => Pg.rowNumber.over(WindowSpec.partitionBy(Param[String]))).compile
    val _: QueryTemplate[String, Long] = q
    assert(q.fragment.sql.contains("PARTITION BY $1"), q.fragment.sql)
  }

  test("WindowSpec.orderBy(Param.desc) threads typed Args via Concat") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users.select(_ => Pg.rank.over(WindowSpec.orderBy(Param[Int].desc))).compile
    val _: QueryTemplate[Int, Long] = q
    assert(q.fragment.sql.contains("ORDER BY $1 DESC"), q.fragment.sql)
  }

  test("WindowSpec partitionBy(Param) + orderBy(Param.asc) threads both Args") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users
      .select(u => Pg.sum(u.age).over(WindowSpec.partitionBy(Param[String]).orderBy(Param[Int].asc)))
      .compile
    val _: QueryTemplate[(String, Int), Long] = q
    assert(q.fragment.sql.contains("PARTITION BY $1 ORDER BY $2 ASC"), q.fragment.sql)
  }

  test("WindowSpec chained partitionBy(col).partitionBy(Param) threads Param's Args only") {
    case class User(id: UUID, email: String, age: Int)
    val users = Table.of[User]("users")
    val q = users
      .select(u => Pg.rowNumber.over(WindowSpec.partitionBy(u.email).partitionBy(Param[Int])))
      .compile
    val _: QueryTemplate[Int, Long] = q
    assert(q.fragment.sql.contains("""PARTITION BY "email", $1"""), q.fragment.sql)
  }

  test("DELETE … USING <typed-args subquery> threads inner Param into outer Args") {
    case class User(id: UUID, email: String, age: Int)
    case class Post(id: UUID, user_id: UUID, status: String)
    val users = Table.of[User]("users")
    val posts = Table.of[Post]("posts")
    val activePosts = posts.select.where(p => p.status === Param[String]).alias("ap")
    val cmd = users.delete
      .using(activePosts)
      .where(r => r.users.id ==== r.ap.user_id)
      .compile
    val _: CommandTemplate[String] = cmd
    assert(cmd.fragment.sql.contains("""USING (SELECT "id", "user_id", "status" FROM "posts" WHERE "status" = $1) AS "ap""""), cmd.fragment.sql)
  }

}
