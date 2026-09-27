package skunk.sharp.returning

import skunk.sharp.dsl.*

import java.util.UUID

object ReturningSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Incoming(id: UUID, email: String, age: Int)
  val users    = Table.of[User]("users")
  val incoming = Table.of[Incoming]("incoming")
}

/** The RETURNING family is one shared implementation (#124); MERGE gains `returningNamed` / `returningAll`. */
class ReturningSuite extends munit.FunSuite {
  import ReturningSuite.*

  private val merge =
    users.merge(incoming).on(r => r.users.id === r.incoming.id).whenMatched.update(r => r.users.age := r.incoming.age)

  test("MERGE … RETURNING every target column") {
    val q                                                        = merge.returningAll.compile
    val _: QueryTemplate[?, (id: UUID, email: String, age: Int)] = q
    assert(q.fragment.sql.endsWith("""RETURNING "id", "email", "age""""), q.fragment.sql)
  }

  test("MERGE … RETURNING a named tuple, with a Param") {
    val q =
      merge.returningNamed(r => (action = Pg.mergeAction, id = r.users.id, bump = r.users.age >= Param[Int])).compile
    val _: QueryTemplate[Int, (action: String, id: UUID, bump: Boolean)] = q
    assert(q.fragment.sql.endsWith("""RETURNING merge_action(), "users"."id", "users"."age" >= $1"""), q.fragment.sql)
  }

  test("returningAll on INSERT / UPDATE / DELETE uses the table's cached column list") {
    val ins = users.insert((id = UUID.randomUUID, email = "x", age = 1)).returningAll.compile
    val upd = users.update.set(u => u.age := 2).where(u => u.email === "x").returningAll.compile
    val del = users.delete.where(u => u.email === "x").returningAll.compile
    for (q <- List(ins.fragment.sql, upd.fragment.sql, del.fragment.sql))
      assert(q.endsWith("""RETURNING "id", "email", "age""""), q)
    assert(users.returningAllExpr eq users.returningAllExpr)
  }
}
