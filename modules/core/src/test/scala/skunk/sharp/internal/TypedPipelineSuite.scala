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

  test("typed chain: .where + .orderBy + .limit keeps Args = Int") {
    val pred = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 21)
    val q = users.select
      .where(_ => pred)
      .orderBy(u => u.email.asc)
      .limit(10)
      .compile
    val _: Int = q.args
    assertEquals(q.args, 21)
    assert(q.fragment.sql.contains("ORDER BY"))
    assert(q.fragment.sql.contains("LIMIT 10"))
  }

  test("typed INSERT: row's value tuple is the visible Args") {
    val c = users.insert.insertT((email = "a@x", age = 30)).compile
    val _: (String, Int) = c.args
    assertEquals(c.args, ("a@x", 30))
    assert(c.fragment.sql.startsWith("INSERT INTO"))
    assert(c.fragment.sql.contains("VALUES"))
    val _: skunk.Command[(String, Int)] = c.typedCommand
  }

  test("typed UPDATE: setT then where threads (T, W) Args") {
    val pred = SqlMacros.infixTyped[Int, Int]("=", users.columnsView.age, 99)
    // Note: := on TypedColumn returns SetAssignment (untyped); :=% returns TypedSetAssignment.
    val c = users.update.setT(u => u.email :=% "new@x").where(_ => pred).compile
    val _: (String, Int) = c.args
    assertEquals(c.args, ("new@x", 99))
    assert(c.fragment.sql.startsWith("UPDATE"))
    assert(c.fragment.sql.contains("SET"))
    assert(c.fragment.sql.contains("WHERE"))
  }

  test("typed UPDATE: single setT alone, no where") {
    val c = users.update.setT(u => u.age :=% 99).compile
    val _: Int = c.args
    assertEquals(c.args, 99)
  }

  test("typed DELETE: Args visible as Int on .compile") {
    val pred = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 50)
    val c = users.delete.where(_ => pred).compile
    val _: Int = c.args
    assertEquals(c.args, 50)
    assert(c.fragment.sql.startsWith("DELETE FROM"))
    val _: skunk.Command[Int] = c.typedCommand
  }

  test("typed DELETE chaining two WHEREs accumulates Args = ((Int, String))") {
    val a = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 50)
    val b = SqlMacros.infixTyped[String, String]("=", users.columnsView.email, "test@x")
    val c = users.delete.where(_ => a).where(_ => b).compile
    val _: (Int, String) = c.args
    assertEquals(c.args, (50, "test@x"))
  }

  test("typed .having extends Args to (whereArgs, havingArgs)") {
    val wherePred  = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 18)
    val havingPred = SqlMacros.infixTyped[Int, Int](">", users.columnsView.age, 100)
    val q = users.select
      .where(_ => wherePred)
      .groupBy(u => u.email)
      .having(_ => havingPred)
      .compile
    val _: (Int, Int) = q.args
    assertEquals(q.args, (18, 100))
    assert(q.fragment.sql.contains("WHERE"))
    assert(q.fragment.sql.contains("GROUP BY"))
    assert(q.fragment.sql.contains("HAVING"))
  }

  test("typed chain: .where + .groupBy + .forUpdate Args stable") {
    val pred = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 5)
    val q = users.select
      .where(_ => pred)
      .groupBy(u => u.email)
      .forUpdate
      .compile
    val _: Int = q.args
    assert(q.fragment.sql.contains("GROUP BY"))
    assert(q.fragment.sql.contains("FOR UPDATE"))
  }

  test("two typed WHEREs in chain: Args = ((first, second))") {
    val a = SqlMacros.infixTyped[Int, Int](">=", users.columnsView.age, 18)
    val b = SqlMacros.infixTyped[String, String]("=", users.columnsView.email, "alice")
    val q = users.select.where(_ => a).where(_ => b).compile
    val _: (Int, String) = q.args
    assertEquals(q.args, (18, "alice"))
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
