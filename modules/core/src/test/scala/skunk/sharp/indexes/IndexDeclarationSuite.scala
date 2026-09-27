package skunk.sharp.indexes

import skunk.sharp.{IndexDef, IndexKey, IndexOrder}
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
        "USING btree (account)",
        "USING btree (household_id, booking_date DESC)",
        "USING btree (account) WHERE account IS NOT NULL",
        "USING btree (booking_date DESC NULLS LAST) WHERE account IS NULL"
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

  test("withIndexDef renders methods, operator classes, collations, expressions, INCLUDE, storage and UNIQUE") {
    val t = ledger
      .withIndexDef(
        IndexDef("ix_hnsw", IndexKey.column("account").opclass("vector_cosine_ops"))
          .withMethod("hnsw")
          .withStorage("m" -> "16", "ef_construction" -> "64")
      )
      .withIndexDef(IndexDef("ix_expr", IndexKey.expr("lower(account)"), IndexKey.column("id").desc).unique)
      .withIndexDef(
        IndexDef(
          "ix_cover",
          IndexKey.column("account").collate("C")
        ).include("booking_date").where("account IS NOT NULL")
      )
    assertEquals(
      t.indexes.map(_.definition),
      List(
        "USING hnsw (account vector_cosine_ops) WITH (m='16', ef_construction='64')",
        "UNIQUE USING btree ((lower(account)), id DESC)",
        """USING btree (account COLLATE "C") INCLUDE (booking_date) WHERE account IS NOT NULL"""
      )
    )
  }

  test("withIndexDef rejects unknown key and INCLUDE columns; expression keys aren't checked") {
    intercept[IllegalArgumentException](ledger.withIndexDef(IndexDef("x", IndexKey.column("nope"))))
    intercept[IllegalArgumentException](ledger.withIndexDef(IndexDef("x", IndexKey.column("id")).include("nope")))
    assertEquals(ledger.withIndexDef(IndexDef("x", IndexKey.expr("nope + 1"))).indexes.size, 1)
  }
}
