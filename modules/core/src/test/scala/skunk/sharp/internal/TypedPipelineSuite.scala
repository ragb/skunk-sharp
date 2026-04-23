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
    // `.compile` surfaces it automatically.
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

  test("TypedWhere.&& pairs Args into a twiddle — (Int, String)") {
    val view = users.columnsView
    val a    = SqlMacros.infixTyped[Int, Int](">=", view.age, 18)
    val b    = SqlMacros.infixTyped[String, String]("=", view.email, "alice@example.com")

    import TypedWhere.&&
    val both = a && b
    val _: TypedWhere[(Int, String)] = both
    assertEquals(both.args, (18, "alice@example.com"))
    // Fragment SQL carries two placeholders from the combined encoder.
    assert(both.fragment.sql.contains("$1"))
    assert(both.fragment.sql.contains("$2"))
  }

  test("end-to-end AND: whereTyped(a && b).compile threads Args = (Int, String)") {
    val view = users.columnsView
    val a    = SqlMacros.infixTyped[Int, Int](">=", view.age, 21)
    val b    = SqlMacros.infixTyped[String, String]("=", view.email, "bob@example.com")
    import TypedWhere.&&
    val q    = users.select.where(_ => a && b).compile

    val _: (Int, String) = q.args  // concrete tuple at the call site
    assertEquals(q.args, (21, "bob@example.com"))

    val typedQ: skunk.Query[(Int, String), ?] = q.typedQuery
    assert(typedQ != null)
  }

  test("end-to-end: users.select.where(...).compile threads Args = Int") {
    // The typed WHERE predicate — produced by the macro.
    val pred = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 21)

    // .whereTyped takes the pre-built typed predicate and threads its Args type onto the builder.
    // .compile surfaces that Args on the resulting CompiledQuery. Row type is the match-type-reduced
    // NamedRowOf[...] of the table — we don't ascribe it directly here because writing out that tuple is noisy.
    val q = users.select.where(_ => pred).compile

    // The concrete Args is Int — accessible via type ascription on q.args.
    val _: Int = q.args
    assertEquals(q.args, 21)
    assert(q.fragment.sql.contains("$1"))
    assert(q.fragment.sql.contains("SELECT"))
    assert(q.fragment.sql.contains("FROM"))
    assert(q.fragment.sql.contains("WHERE"))

    // Prepared-query path typed with Int input — the Int ascription on the input slot proves Args reached here.
    val typedQ: skunk.Query[Int, ?] = q.typedQuery
    assert(typedQ != null)
  }

}
