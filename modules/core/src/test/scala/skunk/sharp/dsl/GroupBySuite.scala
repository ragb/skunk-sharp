package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object GroupBySuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])
}

class GroupBySuite extends munit.FunSuite {
  import GroupBySuite.User

  private val users = Table.of[User]("users")

  test("Pg.countAll renders count(*)") {
    val af = users.select(_ => Pg.countAll).compile.af
    assertEquals(af.fragment.sql, """SELECT count(*) FROM "users"""")
  }

  test("Pg.count(col) renders count(col)") {
    val af = users.select(u => Pg.count(u.id)).compile.af
    assertEquals(af.fragment.sql, """SELECT count("id") FROM "users"""")
  }

  test("Pg.countDistinct(col) renders count(DISTINCT col)") {
    val af = users.select(u => Pg.countDistinct(u.email)).compile.af
    assertEquals(af.fragment.sql, """SELECT count(DISTINCT "email") FROM "users"""")
  }

  test("Pg.sum / Pg.avg / Pg.min / Pg.max render the expected SQL") {
    val sumAf = users.select(u => Pg.sum(u.age)).compile.af
    val avgAf = users.select(u => Pg.avg(u.age)).compile.af
    val minAf = users.select(u => Pg.min(u.age)).compile.af
    val maxAf = users.select(u => Pg.max(u.age)).compile.af
    assertEquals(sumAf.fragment.sql, """SELECT sum("age") FROM "users"""")
    assertEquals(avgAf.fragment.sql, """SELECT avg("age") FROM "users"""")
    assertEquals(minAf.fragment.sql, """SELECT min("age") FROM "users"""")
    assertEquals(maxAf.fragment.sql, """SELECT max("age") FROM "users"""")
  }

  test("Pg.stringAgg renders string_agg(expr, sep) and takes sep as a bound parameter") {
    val af = users.select(u => Pg.stringAgg(u.email, ", ")).compile.af
    assertEquals(af.fragment.sql, """SELECT string_agg("email", $1) FROM "users"""")
  }

  test(".groupBy single column") {
    val af = users.select(u => (u.age, Pg.count(u.id))).groupBy(u => u.age).compile.af
    assertEquals(af.fragment.sql, """SELECT "age", count("id") FROM "users" GROUP BY "age"""")
  }

  test(".groupBy multiple columns (tuple)") {
    val af = users.select(u => (u.age, u.email, Pg.count(u.id))).groupBy(u => (u.age, u.email)).compile.af
    assertEquals(af.fragment.sql, """SELECT "age", "email", count("id") FROM "users" GROUP BY "age", "email"""")
  }

  test(".having filters aggregated rows") {
    val af = users
      .select(u => (u.age, Pg.count(u.id)))
      .groupBy(u => u.age)
      .having(u => Pg.count(u.id) > 5L)
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT "age", count("id") FROM "users" GROUP BY "age" HAVING count("id") > $1"""
    )
  }

  test("GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET order is stable") {
    val af = users.select
      .where(u => u.deleted_at.isNull)
      .apply(u => (u.age, Pg.count(u.id)))
      .groupBy(u => u.age)
      .having(u => Pg.count(u.id) >= 1L)
      .limit(10)
      .offset(5)
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT "age", count("id") FROM "users" WHERE "deleted_at" IS NULL GROUP BY "age" HAVING count("id") >= $1 LIMIT 10 OFFSET 5"""
    )
  }

  test(".groupBy also works on the non-projected whole-row SelectBuilder") {
    val af = users.select.groupBy(u => u.age).compile.af
    assert(af.fragment.sql.contains("""GROUP BY "age""""), af.fragment.sql)
  }

  test(".having on SelectBuilder works with or without an explicit .groupBy (implicit grouping)") {
    val af = users.select.having(_ => Pg.countAll > 0L).compile.af
    assert(af.fragment.sql.contains("""HAVING count(*) > $1"""), af.fragment.sql)
  }
}
