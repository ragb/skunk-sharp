package skunk.sharp.dynamicaf

import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID

object DynamicAfSuite {
  case class User(id: UUID, email: String, age: Int, createdAt: OffsetDateTime)
  val users = Table.of[User]("users").withPrimary("id").withDefault("id").withDefault("createdAt")

  val activeCte = cte("active", users.select.where(u => u.age >= Param[Int]))
  val lookup    = Values.of((id = 1, label = "alpha"), (id = 2, label = "beta")).alias("lookup")
}

/**
 * The 0-dynamic-AppliedFragment invariant, per shape (#123): after a warm-up compile, compiling the same shape again
 * must not build SQL from runtime strings (`RawConstants.rawDynamic`).
 */
class DynamicAfSuite extends munit.FunSuite {
  import DynamicAfSuite.*

  private def noDynamicAfs(name: String)(build: => Any): Unit = {
    val counter = skunk.sharp.internal.RawConstants.rawDynamicThreadCount
    build
    val before = counter.get
    (1 to 20).foreach(_ => build)
    assertEquals(counter.get - before, 0L, name)
  }

  test("row locking") {
    noDynamicAfs("FOR UPDATE SKIP LOCKED")(users.select.where(u => u.age >= 18).forUpdate.skipLocked.compile)
  }

  test("an aliased relation in FROM") {
    noDynamicAfs("alias")(users.alias("u").select.where(u => u.age >= 18).compile)
  }

  test("a CTE and the WITH preamble") {
    noDynamicAfs("cte")(activeCte.select.compile)
    noDynamicAfs("cte re-aliased")(activeCte.alias("a").select.compile)
  }

  test("INSERT into a subset of columns") {
    noDynamicAfs("subset insert")(users.insert((email = "x", age = 1)).compile)
  }

  test("multi-row INSERT … VALUES and a VALUES relation") {
    noDynamicAfs("insert.values")(users.insert.values((email = "a", age = 1), (email = "b", age = 2)).compile)
    noDynamicAfs("VALUES in FROM")(lookup.select.compile)
  }

  test("whole-row DISTINCT ON") {
    noDynamicAfs("distinctOn")(users.select.distinctOn(u => (u.email, u.age)).compile)
  }
}
