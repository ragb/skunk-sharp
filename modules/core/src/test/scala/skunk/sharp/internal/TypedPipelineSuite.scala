package skunk.sharp.internal

import skunk.sharp.dsl.*

import java.util.UUID

/**
 * Demonstrates the end-to-end typed pipeline: `TypedWhere[Args]` → `CompiledQuery[Args, R]` with a concrete
 * visible `Args` tuple at the call site. Only one narrow slice is wired through today — `SqlMacros.infixTyped`
 * + `CompiledQuery.mk` — so this test shows the shape the bigger refactor will reach.
 */
class TypedPipelineSuite extends munit.FunSuite {

  case class User(id: UUID, email: String, age: Int)
  private val users = Table.of[User]("users").withPrimary("id").withDefault("id")

  test("infixTyped carries the RHS type on the produced TypedWhere") {
    val view   = users.columnsView
    val pred   = SqlMacros.infixTyped[Int, Int]("=", view.age, 42)
    val _: TypedWhere[Int] = pred  // Args = Int, visible
    assert(pred.fragment.sql.contains("$1"))
    assertEquals(pred.args, 42)
  }

  test("CompiledQuery.mk carries a concrete Args tuple forward") {
    val view = users.columnsView
    val pred = SqlMacros.infixTyped[Int, Int](">=", view.age, 18)

    // Key point: staying at the `Fragment[Int]` level — NOT crossing into AppliedFragment —
    // preserves Args = Int through to CompiledQuery.mk. The user can ascribe a concrete
    // CompiledQuery[Int, Int] here. When the builder chain threads Args the same way,
    // `.compileTyped` surfaces it automatically.
    val bool = skunk.codec.all.bool
    val q: CompiledQuery[Int, Boolean] =
      CompiledQuery.mk[Int, Boolean](pred.fragment, pred.args, bool)

    // Args really is Int at the call site — the ascription above proves it.
    assertEquals(q.args, 18)
    assert(q.fragment.sql.contains("$1"))

    // And `.typedQuery: Query[Int, Boolean]` is directly usable.
    val typedQ: skunk.Query[Int, Boolean] = q.typedQuery
    assert(typedQ != null)
  }

}
