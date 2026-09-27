package skunk.sharp.indexes

import skunk.sharp.{IndexDef, IndexOrder}
import skunk.sharp.dsl.*

import java.time.LocalDate
import java.util.UUID
import scala.compiletime.testing.typeCheckErrors

object IndexDeclarationSuite {
  case class Ledger(id: Long, household_id: UUID, booking_date: LocalDate, account: Option[String])
  val ledger = Table.of[Ledger]("ledger")
}

/** Declaring non-unique indexes on a `Table` (#94). */
class IndexDeclarationSuite extends munit.FunSuite {
  import IndexDeclarationSuite.ledger

  test("withIndex / withSortedIndex / withPartialIndex record normalised definitions, in order") {
    val t = ledger
      .withIndex["ix_a", Tuple1["account"]]
      .withSortedIndex["ix_b", ("household_id", "booking_date")]((IndexOrder.Asc, IndexOrder.Desc))
      .withPartialIndex["ix_c", Tuple1["account"]]("account IS NOT NULL")
      .withSortedIndex["ix_d", Tuple1["booking_date"]](Tuple1(IndexOrder.DescNullsLast), where = "account IS NULL")
    assertEquals(
      t.indexes.map(_.definition),
      List(
        "btree (account)",
        "btree (household_id, booking_date DESC)",
        "btree (account) WHERE account IS NOT NULL",
        "btree (booking_date DESC NULLS LAST) WHERE account IS NULL"
      )
    )
    assertEquals(t.indexes.map(_.name), List("ix_a", "ix_b", "ix_c", "ix_d"))
    assert(t.indexes.forall(_.isInstanceOf[IndexDef]))
  }

  test("an unknown column, or an order tuple of the wrong arity, doesn't compile") {
    val unknown = typeCheckErrors("""
      import skunk.sharp.dsl.*
      IndexDeclarationSuite.ledger.withIndex["ix", Tuple1["nope"]]
    """)
    assert(unknown.nonEmpty)
    val arity = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import skunk.sharp.IndexOrder
      IndexDeclarationSuite.ledger.withSortedIndex["ix", ("household_id", "booking_date")](Tuple1(IndexOrder.Desc))
    """)
    assert(arity.nonEmpty)
  }

  test("index declarations survive other builder steps") {
    val t = ledger.withIndex["ix_a", Tuple1["account"]].withPrimary("id").withDefault("id")
    assertEquals(t.indexes.map(_.name), List("ix_a"))
  }
}
