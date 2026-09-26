package skunk.sharp.arith

import skunk.sharp.TypedExpr
import skunk.sharp.dsl.*

import java.time.{Duration, LocalDate, LocalDateTime, OffsetDateTime}
import scala.compiletime.testing.*

object ArithSuite {

  case class Item(
    id: Int,
    qty: Int,
    small: Short,
    big: Long,
    price: BigDecimal,
    weight: Double,
    ratio: Float,
    discount: Option[Int],
    name: String,
    due: LocalDate,
    at: OffsetDateTime,
    local: LocalDateTime,
    span: Duration
  )

  val items = Table.of[Item]("items")
}

/** Outside `skunk.sharp.dsl`: this is how user code sees the operators. */
class ArithSuite extends munit.FunSuite {
  import ArithSuite.*

  test("literals, Params and columns; every operation is parenthesised") {
    val q = items.select(i => (i.qty + 1, i.qty - Param[Int], i.qty * i.qty, i.qty / 2, i.qty % 3, -i.qty)).compile
    assertEquals(
      q.fragment.sql,
      """SELECT ("qty" + 1), ("qty" - $1), ("qty" * "qty"), ("qty" / 2), ("qty" % 3), (- "qty") FROM "items""""
    )
    val _: QueryTemplate[Int, (Int, Int, Int, Int, Int, Int)] = q
  }

  test("nesting renders correctly whatever the Scala grouping") {
    val q = items.select(i => (i.qty + i.qty * 2) * (i.qty - 1)).compile
    assertEquals(q.fragment.sql, """SELECT (("qty" + ("qty" * 2)) * ("qty" - 1)) FROM "items"""")
  }

  test("arithmetic works in WHERE and ORDER BY too") {
    val q = items.select(i => i.id).where(i => i.qty * 2 > Param[Int]).orderBy(i => (i.qty - i.small).desc).compile
    assertEquals(
      q.fragment.sql,
      """SELECT "id" FROM "items" WHERE ("qty" * 2) > $1 ORDER BY ("qty" - "small") DESC"""
    )
  }

  test("numeric promotion follows Postgres") {
    val _: TypedExpr[Int, ?]        = items.columnsView.small + items.columnsView.qty
    val _: TypedExpr[Long, ?]       = items.columnsView.qty + items.columnsView.big
    val _: TypedExpr[BigDecimal, ?] = items.columnsView.big * items.columnsView.price
    val _: TypedExpr[Double, ?]     = items.columnsView.price - items.columnsView.weight
    val _: TypedExpr[Double, ?]     = items.columnsView.ratio * items.columnsView.weight
    val _: TypedExpr[Float, ?]      = items.columnsView.ratio + items.columnsView.ratio
    val _: TypedExpr[Int, ?]        = items.columnsView.qty / items.columnsView.qty // truncates, as in Postgres
  }

  test("a nullable operand makes the result Option") {
    val _: TypedExpr[Option[Int], ?] = items.columnsView.qty - items.columnsView.discount
    val _: TypedExpr[Option[Int], ?] = items.columnsView.discount * 2
  }

  test("date / time arithmetic") {
    val c                               = items.columnsView
    val _: TypedExpr[OffsetDateTime, ?] = c.at + c.span
    val _: TypedExpr[LocalDateTime, ?]  = c.local - c.span
    val _: TypedExpr[LocalDate, ?]      = c.due + 7
    val _: TypedExpr[Int, ?]            = c.due - c.due
    val _: TypedExpr[Duration, ?]       = c.at - c.at
    val _: TypedExpr[LocalDateTime, ?]  = c.due + c.span
    val _: TypedExpr[Duration, ?]       = c.span * 2.0
    val q                               = items.select(i => (i.due + 30, i.at - Param[Duration])).compile
    assertEquals(q.fragment.sql, """SELECT ("due" + 30), ("at" - $1) FROM "items"""")
  }

  test("text arithmetic, float modulo and date + date don't compile") {
    assert(typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArithSuite.items
      items.select(i => i.name + 1)
    """).nonEmpty)
    assert(typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArithSuite.items
      items.select(i => i.weight % 2.0)
    """).nonEmpty)
    assert(typeCheckErrors("""
      import skunk.sharp.dsl.*
      import ArithSuite.items
      items.select(i => i.due + i.due)
    """).nonEmpty)
  }

  test("an instance for one operator doesn't satisfy another") {
    // pgvector ships + - * only: / and unary - must be clean "no instance" errors, not ambiguity or a pass.
    val div = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import skunk.sharp.contrib.pgvector.*
      import skunk.sharp.contrib.PgVectorSuite.chunks
      chunks.select(c => c.embedding / c.embedding)
    """).map(_.message).mkString("\n")
    assert(div.nonEmpty && !div.contains("mbiguous"), div)
    val neg = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import skunk.sharp.contrib.pgvector.*
      import skunk.sharp.contrib.PgVectorSuite.chunks
      chunks.select(c => -c.embedding)
    """)
    assert(neg.nonEmpty)
    // A user type with only `Plus` gets `+` and nothing else.
    val times = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import skunk.sharp.arith.ArithSuiteMoney.*
      wallets.select(w => w.balance * w.balance)
    """)
    assert(times.nonEmpty)
    val userDiv = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import skunk.sharp.arith.ArithSuiteMoney.*
      wallets.select(w => w.balance / w.balance)
    """)
    assert(userDiv.nonEmpty)
    val plus = ArithSuiteMoney.wallets.select(w => w.balance + w.balance).compile
    assertEquals(plus.fragment.sql, """SELECT ("balance" + "balance") FROM "wallets"""")
  }

  test("unary minus on numbers and intervals; Short in modulo") {
    val q = items.select(i => (-i.price, -i.span, i.small % i.qty, i.big % i.small)).compile
    val _: QueryTemplate[skunk.Void, (BigDecimal, Duration, Int, Long)] = q
    assertEquals(
      q.fragment.sql,
      """SELECT (- "price"), (- "span"), ("small" % "qty"), ("big" % "small") FROM "items""""
    )
  }
}

/** A user-defined type with only an addition instance. */
object ArithSuiteMoney {
  final case class Money(cents: Long)

  object Money {

    given skunk.sharp.pg.PgTypeFor[Money] =
      skunk.sharp.pg.PgTypeFor.instance(skunk.codec.all.int8.imap(Money(_))(_.cents))

    given skunk.sharp.ops.Plus.Aux[Money, Money, Money] = skunk.sharp.ops.Plus.of
  }

  case class Wallet(id: Int, balance: Money)
  val wallets = Table.of[Wallet]("wallets")
}
