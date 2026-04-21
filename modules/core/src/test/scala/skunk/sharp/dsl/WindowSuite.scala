package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object WindowSuite {
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
}

class WindowSuite extends munit.FunSuite {
  import WindowSuite.User

  private val users = Table.of[User]("users")

  // ---- Ranking functions -----------------------------------------------------------------------

  test("row_number() OVER () — empty spec") {
    val af = users.select(_ => Pg.rowNumber.over()).compile.af
    assertEquals(af.fragment.sql, """SELECT row_number() OVER () FROM "users"""")
  }

  test("row_number() OVER (ORDER BY age ASC)") {
    val af = users.select(u => Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc))).compile.af
    assertEquals(af.fragment.sql, """SELECT row_number() OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("row_number() OVER (PARTITION BY email ORDER BY age DESC)") {
    val af = users
      .select(u => Pg.rowNumber.over(WindowSpec.partitionBy(u.email).orderBy(u.age.desc)))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT row_number() OVER (PARTITION BY "email" ORDER BY "age" DESC) FROM "users""""
    )
  }

  test("rank() OVER (ORDER BY age DESC)") {
    val af = users.select(u => Pg.rank.over(WindowSpec.orderBy(u.age.desc))).compile.af
    assertEquals(af.fragment.sql, """SELECT rank() OVER (ORDER BY "age" DESC) FROM "users"""")
  }

  test("dense_rank() OVER (ORDER BY age)") {
    val af = users.select(u => Pg.denseRank.over(WindowSpec.orderBy(u.age.asc))).compile.af
    assertEquals(af.fragment.sql, """SELECT dense_rank() OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("percent_rank() OVER (ORDER BY age)") {
    val af = users.select(u => Pg.percentRank.over(WindowSpec.orderBy(u.age.asc))).compile.af
    assertEquals(af.fragment.sql, """SELECT percent_rank() OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("cume_dist() OVER (ORDER BY age)") {
    val af = users.select(u => Pg.cumeDist.over(WindowSpec.orderBy(u.age.asc))).compile.af
    assertEquals(af.fragment.sql, """SELECT cume_dist() OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("ntile(4) OVER (ORDER BY age)") {
    val af = users.select(u => Pg.ntile(4).over(WindowSpec.orderBy(u.age.asc))).compile.af
    assertEquals(af.fragment.sql, """SELECT ntile(4) OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  // ---- Aggregate as window function ------------------------------------------------------------

  test("sum(age) OVER (PARTITION BY email) — running sum") {
    val af = users
      .select(u => Pg.sum(u.age).over(WindowSpec.partitionBy(u.email)))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT sum("age") OVER (PARTITION BY "email") FROM "users""""
    )
  }

  test("count(*) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)") {
    val af = users
      .select(u =>
        Pg.countAll.over(
          WindowSpec
            .orderBy(u.age.asc)
            .rowsBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow)
        )
      )
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT count(*) OVER (ORDER BY "age" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "users""""
    )
  }

  test("avg(age) OVER (PARTITION BY email ORDER BY age ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)") {
    val af = users
      .select(u =>
        Pg.avg(u.age).over(
          WindowSpec
            .partitionBy(u.email)
            .orderBy(u.age.asc)
            .rowsBetween(FrameBound.Preceding(2), FrameBound.Following(2))
        )
      )
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT avg("age") OVER (PARTITION BY "email" ORDER BY "age" ASC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM "users""""
    )
  }

  test("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW") {
    val af = users
      .select(u =>
        Pg.countAll.over(
          WindowSpec.orderBy(u.age.asc).rangeBetween(FrameBound.UnboundedPreceding, FrameBound.CurrentRow)
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"), af.fragment.sql)
  }

  test("GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING") {
    val af = users
      .select(u =>
        Pg.countAll.over(
          WindowSpec.orderBy(u.age.asc).groupsBetween(FrameBound.CurrentRow, FrameBound.UnboundedFollowing)
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"), af.fragment.sql)
  }

  // ---- Offset access functions -----------------------------------------------------------------

  test("lag(age) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.lag(u.age).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT lag("age") OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("lag(age, 2) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.lag(u.age, 2).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT lag("age", 2) OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("lag(age, 1, 0) OVER (ORDER BY age) — with default, non-optional result") {
    val af = users
      .select(u => Pg.lag(u.age, 1, 0).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT lag("age", 1, $1) OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("lead(age) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.lead(u.age).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT lead("age") OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("lead(age, 1, 0) OVER (ORDER BY age) — with default") {
    val af = users
      .select(u => Pg.lead(u.age, 1, 0).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT lead("age", 1, $1) OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  // ---- Value functions -------------------------------------------------------------------------

  test("first_value(age) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.firstValue(u.age).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT first_value("age") OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("last_value(age) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.lastValue(u.age).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT last_value("age") OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  test("nth_value(age, 2) OVER (ORDER BY age)") {
    val af = users
      .select(u => Pg.nthValue(u.age, 2).over(WindowSpec.orderBy(u.age.asc)))
      .compile.af
    assertEquals(af.fragment.sql, """SELECT nth_value("age", 2) OVER (ORDER BY "age" ASC) FROM "users"""")
  }

  // ---- Multi-expression projection ------------------------------------------------------------

  test("window function in tuple projection alongside a column") {
    val af = users
      .select(u => (u.email, Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc))))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "email", row_number() OVER (ORDER BY "age" ASC) FROM "users""""
    )
  }

  // ---- Type-level checks -----------------------------------------------------------------------

  test("lag without default decodes as Option[Int]") {
    val _: CompiledQuery[Option[Int]] =
      users.select(u => Pg.lag(u.age).over(WindowSpec.orderBy(u.age.asc))).compile
  }

  test("lag with default decodes as Int (non-optional)") {
    val _: CompiledQuery[Int] =
      users.select(u => Pg.lag(u.age, 1, 0).over(WindowSpec.orderBy(u.age.asc))).compile
  }

  test("row_number decodes as Long") {
    val _: CompiledQuery[Long] =
      users.select(u => Pg.rowNumber.over(WindowSpec.orderBy(u.age.asc))).compile
  }

}
