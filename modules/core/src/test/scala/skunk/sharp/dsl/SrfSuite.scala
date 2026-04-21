package skunk.sharp.dsl

import skunk.data.Arr
import skunk.sharp.dsl.*

import java.util.UUID

object SrfSuite {
  case class User(id: UUID, email: String, tags: Arr[String])
}

class SrfSuite extends munit.FunSuite {
  import SrfSuite.*

  private val users = Table.of[User]("users")

  test("Pg.generateSeries(1, 10) renders as `generate_series($1, $2) AS \"n\"(\"n\")` in FROM") {
    val af = Pg.generateSeries(1, 10).select.compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "n" FROM generate_series($1, $2) AS "n"("n")"""
    )
  }

  test("Pg.generateSeries 3-arg form includes the step") {
    val af = Pg.generateSeries(0, 10, 2).select.compile.af
    assert(af.fragment.sql.contains("""generate_series($1, $2, $3)"""), af.fragment.sql)
  }

  test("SRF relation joined with a base table — CROSS JOIN form") {
    val af = users
      .crossJoin(Pg.generateSeries(1, 3).alias("g"))
      .select(r => (r.users.email, r.g.n))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "g"."n" FROM "users" CROSS JOIN generate_series($1, $2) AS "g"("n")"""
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
    val af = Pg.generateSeries(1, 10)
      .select
      .where(g => g.n >= 5)
      .orderBy(g => g.n.desc)
      .compile
      .af

    assert(af.fragment.sql.contains("""WHERE "n" >= $3"""), af.fragment.sql)
    assert(af.fragment.sql.endsWith(""" ORDER BY "n" DESC"""), af.fragment.sql)
  }
}
