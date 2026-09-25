package skunk.sharp.dsl

import skunk.data.Arr
import skunk.sharp.dsl.*

import java.util.UUID

object SrfSuite {
  case class User(id: UUID, email: String, tags: Arr[String])
  case class Item(name: String, qty: Int)
}

class SrfSuite extends munit.FunSuite {
  import SrfSuite.*

  private val users = Table.of[User]("users")

  test("Pg.generateSeries with literal bounds renders inline `generate_series(1, 10)`") {
    val af = Pg.generateSeries(lit(1), lit(10)).select.compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "n" FROM generate_series(1, 10) AS "n"("n")"""
    )
  }

  test("Pg.generateSeries 3-arg form includes the step inline") {
    val af = Pg.generateSeries(lit(0), lit(10), lit(2)).select.compile.af
    assert(af.fragment.sql.contains("""generate_series(0, 10, 2)"""), af.fragment.sql)
  }

  test("Pg.generateSeries with Param bounds threads typed Args into outer compile") {
    val qt                              = Pg.generateSeries(Param[Int], Param[Int]).select.compile
    val _: QueryTemplate[(Int, Int), ?] = qt
    assert(qt.fragment.sql.contains("""generate_series($1, $2)"""), qt.fragment.sql)
  }

  test("SRF relation joined with a base table — CROSS JOIN form") {
    val af = users
      .crossJoin(Pg.generateSeries(lit(1), lit(3)).alias("g"))
      .select(r => (r.users.email, r.g.n))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "g"."n" FROM "users" CROSS JOIN generate_series(1, 3) AS "g"("n")"""
    )
  }

  test("Pg.unnestAsRelation in a LATERAL join — expands an array column per outer row") {
    val af = users
      .innerJoinLateral(u => Pg.unnestAsRelation(u.tags).alias("t"))
      .on(_ => lit(true))
      .select(r => (r.users.email, r.t.v))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "t"."v" FROM "users" INNER JOIN LATERAL unnest("users"."tags") AS "t"("v") ON TRUE"""
    )
  }

  test("SRF output column is usable in WHERE / ORDER BY") {
    val af = Pg.generateSeries(lit(1), lit(10))
      .select
      .where(g => g.n >= lit(5))
      .orderBy(g => g.n.desc)
      .compile
      .af

    assert(af.fragment.sql.contains("""WHERE "n" >= 5"""), af.fragment.sql)
    assert(af.fragment.sql.endsWith(""" ORDER BY "n" DESC"""), af.fragment.sql)
  }

  // ---- Multi-array unnest ----

  test("multi-array unnest zips typed array Params into one relation") {
    val q: QueryTemplate[(List[String], List[Int]), (String, Int)] =
      Pg.unnestAsRelation((name = Param[List[String]], qty = Param[List[Int]]))
        .alias("incoming")
        .select(r => (r.name, r.qty))
        .compile
    assertEquals(
      q.fragment.sql,
      """SELECT "incoming"."name", "incoming"."qty" FROM unnest($1, $2) AS "incoming"("name", "qty")"""
    )
  }

  test("multi-array unnest joins like any relation") {
    val q = users
      .innerJoin(Pg.unnestAsRelation((email = Param[List[String]], score = Param[List[Int]])).alias("u2"))
      .on(r => r.users.email === r.u2.email)
      .select(r => (r.users.id, r.u2.score))
      .compile
    val _: QueryTemplate[(List[String], List[Int]), (UUID, Int)] = q
    assert(
      q.fragment.sql.endsWith(
        """FROM "users" INNER JOIN unnest($1, $2) AS "u2"("email", "score") ON "users"."email" = "u2"."email""""
      ),
      q.fragment.sql
    )
  }

  test("SRF and typed-subquery sources allocate no dynamic AppliedFragments per compile") {
    // Per-thread counter: other suites run in parallel and may build dynamic fragments on purpose.
    val counter = skunk.sharp.internal.RawConstants.rawDynamicThreadCount
    // Relations are declared once, like tables; the per-relation projection cache (`starProjAf`) then warms once.
    val incoming = Pg.unnestAsRelation((name = Param[List[String]], qty = Param[List[Int]])).alias("incoming")
    val sub      = users.select.where(u => u.email === Param[String]).alias("sub")
    def build()  = {
      incoming.select.compile
      users.innerJoin(sub).on(r => r.users.id === r.sub.id).select(r => r.users.email).compile
    }
    build() // warm the intern table
    val before = counter.get
    (1 to 50).foreach(_ => build())
    assertEquals(counter.get - before, 0L)
  }

  // ---- unnestRows: the batch as one List[Row] parameter ----

  test("unnestRows[CaseClass] takes the whole batch as one List parameter, split into per-field arrays") {
    val q: QueryTemplate[List[Item], (String, Int)] =
      Pg.unnestRows[Item].alias("i").select(r => (r.name, r.qty)).compile
    assertEquals(q.fragment.sql, """SELECT "i"."name", "i"."qty" FROM unnest($1, $2) AS "i"("name", "qty")""")
    assertEquals(
      q.fragment.encoder.encode(List(Item("a", 1), Item("b", 2))).flatten.map(_.value),
      List("{\"a\",\"b\"}", "{\"1\",\"2\"}")
    )
  }

  test("unnestRows accepts a named-tuple row type too") {
    val q = Pg.unnestRows[(name: String, qty: Int)].alias("i").select(r => r.qty).compile
    val _: QueryTemplate[List[(name: String, qty: Int)], Int] = q
    assertEquals(
      q.fragment.encoder.encode(List((name = "a", qty = 7))).flatten.map(_.value),
      List("{\"a\"}", "{\"7\"}")
    )
  }

  test("multi-array unnest rejects arrays of different lengths when encoding") {
    val q = Pg.unnestAsRelation((name = Param[List[String]], qty = Param[List[Int]])).alias("i").select.compile
    val e = intercept[IllegalArgumentException](q.fragment.encoder.encode((List("a", "b"), List(1))))
    assert(e.getMessage.contains("same length (got 2, 1)"), e.getMessage)
  }
}
