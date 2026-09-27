package skunk.sharp.alloft

import skunk.AppliedFragment
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object AllOfTupleSuite {
  case class User(id: UUID, email: String, age: Int, deleted_at: Option[OffsetDateTime])
  val users = Table.of[User]("users")
}

/** `allOfT` / `anyOfT`: fixed-arity AND / OR over predicates with different Args (#77). */
class AllOfTupleSuite extends munit.FunSuite {
  import AllOfTupleSuite.users

  private val c = users.columnsView

  private def encoded(af: AppliedFragment): List[Option[String]] =
    af.fragment.encoder.encode(af.argument).map(_.map(_.value))

  test("allOfT folds the items' Args, dropping Void ones") {
    val w: Where[(Int, String)] = allOfT((c.age >= Param[Int], c.email === Param[String], c.deleted_at.isNull))
    assertEquals(w.fragment.sql, """("age" >= $1 AND "email" = $2 AND "deleted_at" IS NULL)""")
  }

  test("anyOfT renders OR; nests inside allOfT and composes with &&") {
    val q = users.select
      .where(u => allOfT((u.age >= Param[Int], anyOfT((u.email === Param[String], u.email === "admin")))))
      .compile
    val _: QueryTemplate[(Int, String), ?] = q
    assert(
      q.fragment.sql.endsWith("""WHERE ("age" >= $1 AND ("email" = $2 OR "email" = 'admin'))"""),
      q.fragment.sql
    )
    assertEquals(encoded(q.bind((18, "x"))), List(Some("18"), Some("x")))
  }

  test("single-item and all-Void tuples") {
    val one: Where[Int] = allOfT(Tuple1(c.age >= Param[Int]))
    assertEquals(one.fragment.sql, """("age" >= $1)""")
    val void: Where[skunk.Void] = anyOfT((c.deleted_at.isNull, c.age >= 18))
    assertEquals(void.fragment.sql, """("deleted_at" IS NULL OR "age" >= 18)""")
  }

  test("named params flow through") {
    val q = users.select
      .where(u => allOfT((u.age >= Param.named["min", Int], u.age <= Param.named["max", Int])))
      .compile
    assertEquals(encoded(q.bind((min = 18, max = 65))), List(Some("18"), Some("65")))
  }

  test("no dynamic AppliedFragments") {
    val counter = skunk.sharp.internal.RawConstants.rawDynamicThreadCount
    def build() = users.select.where(u => allOfT((u.age >= Param[Int], u.email === Param[String]))).compile
    build()
    val before = counter.get
    (1 to 20).foreach(_ => build())
    assertEquals(counter.get - before, 0L)
  }
}
