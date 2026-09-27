package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.IndexOrder
import skunk.sharp.dsl.*
import skunk.sharp.validation.Mismatch

import java.time.LocalDate
import java.util.UUID

object IndexValidationSuite {
  case class Ledger(id: Long, household_id: UUID, booking_date: LocalDate, account: Option[String])
}

/** Declared non-unique indexes against `pg_index` (V16__indexes.sql). */
class IndexValidationSuite extends PgFixture {
  import IndexValidationSuite.*

  private val base = Table.of[Ledger]("ledger").withPrimary("id")

  private val declared = base
    .withSortedIndex["ledger_household_booking_idx", ("household_id", "booking_date", "id")](
      (IndexOrder.Asc, IndexOrder.Desc, IndexOrder.Desc)
    )
    .withPartialIndex["ledger_account_linked_idx", Tuple1["account"]]("account IS NOT NULL")
    .withIndex["ledger_booking_idx", Tuple1["booking_date"]]

  private def report(t: Table[?, ?]) =
    withContainers(containers => session(containers).use(s => SchemaValidator.validate[IO](s, t)))

  test("matching declarations — composite with DESC keys, partial, plain — validate clean") {
    report(declared).map(r => assert(r.isValid, r.mismatches.map(_.pretty).mkString("; ")))
  }

  test("a table that declares no indexes isn't index-checked") {
    report(base).map(r => assert(r.isValid, r.mismatches.map(_.pretty).mkString("; ")))
  }

  test("once a table declares indexes, an undeclared one is reported") {
    val partial = base.withIndex["ledger_booking_idx", Tuple1["booking_date"]]
    report(partial).map { r =>
      val extras = r.mismatches.collect { case Mismatch.ExtraIndex(_, name, _) => name }.toSet
      assertEquals(extras, Set("ledger_household_booking_idx", "ledger_account_linked_idx"))
    }
  }

  test("a wrong key order, a wrong predicate and a missing index are reported") {
    val wrong = base
      .withIndex["ledger_household_booking_idx", ("household_id", "booking_date", "id")] // DB has DESC keys
      .withPartialIndex["ledger_account_linked_idx", Tuple1["account"]]("account IS NULL")
      .withIndex["ledger_booking_idx", Tuple1["booking_date"]]
      .withIndex["ledger_nope_idx", Tuple1["account"]]
    report(wrong).map { r =>
      val mismatched = r.mismatches.collect { case Mismatch.IndexDefinitionMismatch(_, n, _, _) => n }.toSet
      val missing    = r.mismatches.collect { case Mismatch.IndexMissing(_, n, _) => n }.toSet
      assertEquals(mismatched, Set("ledger_household_booking_idx", "ledger_account_linked_idx"))
      assertEquals(missing, Set("ledger_nope_idx"))
      val pretty = r.mismatches.map(_.pretty).mkString("\n")
      assert(pretty.contains("btree (household_id, booking_date DESC, id DESC)"), pretty)
    }
  }
}
