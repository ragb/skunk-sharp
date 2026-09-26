package skunk.sharp.orderby

import skunk.sharp.dsl.*
import skunk.Void

import java.util.UUID

object WholeRowOrderBySuite {
  case class User(id: UUID, email: String, age: Int)
  val users = Table.of[User]("users")
}

class WholeRowOrderBySuite extends munit.FunSuite {
  import WholeRowOrderBySuite.users

  /** Run the bound encoder — slot misalignment shows up here as a ClassCastException. */
  private def encoded(af: skunk.AppliedFragment): List[Option[String]] =
    af.fragment.encoder.encode(af.argument).map(_.map(_.value))

  private val star = """SELECT "id", "email", "age" FROM "users""""

  test("a Void whole-row orderBy keeps the query Void") {
    val q                         = users.select.orderBy(u => u.age.desc).compile
    val _: QueryTemplate[Void, ?] = q
    assertEquals(q.fragment.sql.trim, s"""$star ORDER BY "age" DESC""")
  }

  test("a deferred Param in a whole-row orderBy is threaded into Args") {
    val q                           = users.select.orderBy(u => (u.email === Param[String]).desc).compile
    val _: QueryTemplate[String, ?] = q
    assertEquals(q.fragment.sql.trim, s"""$star ORDER BY "email" = $$1 DESC""")
  }

  test("ORDER BY Args bind after WHERE Args, in SQL order") {
    val q = users.select
      .where(u => u.age >= Param[Int])
      .orderBy(u => ((u.email === Param[String]).desc, u.id.asc))
      .limit(10)
      .compile
    val _: QueryTemplate[(Int, String), ?] = q
    assertEquals(
      q.fragment.sql.trim,
      s"""$star WHERE "age" >= $$1 ORDER BY "email" = $$2 DESC, "id" ASC LIMIT 10"""
    )
  }

  test("a tuple of parameterised items and successive .orderBy calls fold into one flat tuple") {
    val q = users.select
      .orderBy(u => ((u.email === Param[String]).desc, (u.age === Param[Int]).desc))
      .orderBy(u => (u.id === Param[UUID]).asc)
      .compile
    val _: QueryTemplate[(String, Int, UUID), ?] = q
    assertEquals(
      q.fragment.sql.trim,
      s"""$star ORDER BY "email" = $$1 DESC, "age" = $$2 DESC, "id" = $$3 ASC"""
    )
  }

  test("named Params work in a whole-row orderBy") {
    val q = users.select
      .where(u => u.age >= Param.named["min", Int])
      .orderBy(u => (u.email === Param.named["email", String]).desc)
      .compile
    q.bind((min = 18, email = "a@b.c"))
    assertEquals(q.fragment.sql.trim, s"""$star WHERE "age" >= $$1 ORDER BY "email" = $$2 DESC""")
  }

  test("projecting after a parameterised whole-row orderBy keeps its Args") {
    val q = users.select
      .where(u => u.age >= Param[Int])
      .orderBy(u => (u.email === Param[String]).desc)
      .select(u => u.email)
      .compile
    val _: QueryTemplate[(Int, String), String] = q
    assertEquals(
      q.fragment.sql.trim,
      """SELECT "email" FROM "users" WHERE "age" >= $1 ORDER BY "email" = $2 DESC"""
    )
  }

  test("a Void whole-row orderBy followed by a parameterised projected orderBy binds the right slot") {
    val q = users.select
      .orderBy(u => u.age.desc)
      .select(u => u.email)
      .orderBy(u => (u.email === Param[String]).desc)
      .compile
    val _: QueryTemplate[String, String] = q
    assertEquals(q.fragment.sql.trim, """SELECT "email" FROM "users" ORDER BY "age" DESC, "email" = $1 DESC""")
    assertEquals(encoded(q.bind("x")), List(Some("x")))
  }

  test("parameterised orderBy on both sides of the projection hand-off") {
    val q = users.select
      .orderBy(u => (u.age === Param[Int]).desc)
      .select(u => u.email)
      .orderBy(u => (u.email === Param[String]).desc)
      .compile
    val _: QueryTemplate[(Int, String), String] = q
    assertEquals(q.fragment.sql.trim, """SELECT "email" FROM "users" ORDER BY "age" = $1 DESC, "email" = $2 DESC""")
    assertEquals(encoded(q.bind((1, "x"))), List(Some("1"), Some("x")))
  }

  test("ORDER BY Args flow out of an .alias subquery") {
    val sub = users.select.orderBy(u => (u.email === Param[String]).desc).limit(5).alias("u")
    val q   = sub.select.where(u => u.age >= Param[Int]).compile
    val _: QueryTemplate[(String, Int), ?] = q
    assertEquals(
      q.fragment.sql.trim,
      """SELECT "id", "email", "age" FROM (SELECT "id", "email", "age" FROM "users" ORDER BY "email" = $1 DESC LIMIT 5) AS "u" WHERE "age" >= $2"""
    )
  }

  test("ORDER BY Args flow out of a CTE") {
    val top                         = cte("top", users.select.orderBy(u => (u.email === Param[String]).desc).limit(5))
    val q                           = top.select.compile
    val _: QueryTemplate[String, ?] = q
    assert(q.fragment.sql.startsWith("""WITH "top" AS ("""), q.fragment.sql)
    assert(q.fragment.sql.contains("""ORDER BY "email" = $1 DESC LIMIT 5"""), q.fragment.sql)
  }

  test("ORDER BY Args flow out of an IN subquery") {
    val q = users.select
      .where(u => u.id.in(users.select.orderBy(v => (v.email === Param[String]).desc).limit(1).select(v => v.id)))
      .compile
    val _: QueryTemplate[String, ?] = q
    assert(q.fragment.sql.contains("""ORDER BY "email" = $1 DESC LIMIT 1"""), q.fragment.sql)
  }

  test("parameterised whole-row orderBy allocates no dynamic AppliedFragments") {
    val counter = skunk.sharp.internal.RawConstants.rawDynamicThreadCount
    def build() =
      users.select
        .where(u => u.age >= Param[Int])
        .orderBy(u => ((u.email === Param[String]).desc, u.id.asc))
        .limit(10)
        .compile
    build()
    val before = counter.get
    (1 to 50).foreach(_ => build())
    assertEquals(counter.get - before, 0L)
  }
}
