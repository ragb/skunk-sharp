package skunk.sharp.argshape

import skunk.AppliedFragment
import skunk.data.Arr
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given

import java.util.UUID
import scala.compiletime.testing.typeCheckErrors

object ArgsShapeSuite {
  case class User(id: UUID, email: String, age: Int, score: Double)
  case class Tagged(id: UUID, tags: Arr[Int])
  val users  = Table.of[User]("users")
  val tagged = Table.of[Tagged]("tagged")
}

/**
 * Regression tests for Args-shape bugs that compiled but crashed (or mis-encoded) at encode time. Every test runs the
 * bound encoder: rendering the SQL alone doesn't exercise the slot projection.
 */
class ArgsShapeSuite extends munit.FunSuite {
  import ArgsShapeSuite.*

  private def encoded(af: AppliedFragment): List[Option[String]] =
    af.fragment.encoder.encode(af.argument).map(_.map(_.value))

  private val id = UUID.fromString("00000000-0000-0000-0000-000000000001")

  test(".patch with several Some fields encodes every value") {
    val af = users.update
      .patch((email = Some("x"), age = Some(42)))
      .where(u => u.id === Param.bind(id))
      .compile
      .af
    assertEquals(af.fragment.sql, """UPDATE "users" SET "email" = $1, "age" = $2 WHERE "id" = $3""")
    assertEquals(encoded(af), List(Some("x"), Some("42"), Some(id.toString)))
  }

  test("stringAgg with a multi-Param first argument splits Args correctly") {
    val q = users
      .select(_ => Pg.stringAgg(Pg.concat(Param[String], Param[String]), Param[String]))
      .compile
    val _: QueryTemplate[(String, String, String), String] = q
    assertEquals(encoded(q.bind(("a", "b", ","))), List(Some("a"), Some("b"), Some(",")))
  }

  test("two-argument aggregates (corr / regr_*) split multi-Param sides correctly") {
    val q = users
      .select(u => Pg.corr(Pg.coalesce(Param[Double], u.score, Param[Double]), Param[Double]))
      .compile
    val _: QueryTemplate[(Double, Double, Double), Double] = q
    assertEquals(encoded(q.bind((1.0, 2.0, 3.0))).size, 3)
  }

  test("array functions split multi-Param sides correctly") {
    val q = tagged
      .select(_ => Pg.arrayAppend(Pg.arrayCat(Param[Arr[Int]], Param[Arr[Int]]), Param[Int]))
      .compile
    val _: QueryTemplate[(Arr[Int], Arr[Int], Int), Arr[Int]] = q
    assertEquals(encoded(q.bind((Arr(1), Arr(2), 3))).size, 3)
  }

  test("arrayReplace threads typed Args instead of dropping them") {
    val q = tagged.select(t => Pg.arrayReplace(t.tags, Param[Int], Param[Int])).compile
    val _: QueryTemplate[(Int, Int), Arr[Int]] = q
    assertEquals(q.fragment.sql.trim, """SELECT array_replace("tags", $1, $2) FROM "tagged"""")
    assertEquals(encoded(q.bind((1, 2))), List(Some("1"), Some("2")))
  }

  test("variadic grouping specs and format reject a deferred Param at compile time") {
    val rollup = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArgsShapeSuite.users
      users.select.groupBy(u => Pg.rollup(u.age, Param[Int]))
    """)
    assert(rollup.nonEmpty)
    val format = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArgsShapeSuite.users
      users.select(u => Pg.format(lit("%s-%s"), u.email, Param[String]))
    """)
    assert(format.nonEmpty)
  }

  test("format and rollup still accept Void-args items, incl. Param.bind") {
    val af = users.select(u => Pg.format(lit("%s-%s"), u.email, Param.bind("x"))).compile.af
    assertEquals(encoded(af), List(Some("x")))
  }

  test("whole-row distinctOn rejects a deferred Param at compile time") {
    val msg = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArgsShapeSuite.users
      users.select.distinctOn(u => (u.email, u.age === Param[Int]))
    """).map(_.message).mkString("\n")
    assert(msg.contains("whole-row .distinctOn"), msg)
  }

  test("whole-row distinctOn with Void items still works, incl. Param.bind") {
    val af = users.select.distinctOn(u => (u.email, u.age === Param.bind(3))).compile.af
    assertEquals(
      af.fragment.sql.trim,
      """SELECT DISTINCT ON ("email", "age" = $1) "id", "email", "age", "score" FROM "users""""
    )
    assertEquals(encoded(af), List(Some("3")))
  }
}
