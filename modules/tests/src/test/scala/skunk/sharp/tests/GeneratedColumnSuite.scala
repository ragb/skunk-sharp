package skunk.sharp.tests

import cats.effect.IO
import skunk.sharp.dsl.*
import skunk.sharp.validation.Mismatch

object GeneratedColumnSuite {

  // Matches V12__generated_columns.sql. Generated columns read back as nullable in information_schema.
  case class PricedItem(
    id: Int,
    name: String,
    price_net: BigDecimal,
    vat_rate: BigDecimal,
    price_gross: Option[BigDecimal],
    label: Option[String]
  )

}

class GeneratedColumnSuite extends PgFixture {
  import GeneratedColumnSuite.*

  private val items = Table
    .of[PricedItem]("priced_items")
    .withPrimary("id")
    .withDefault("id")
    .withGenerated("price_gross")
    .withGenerated("label")

  test("INSERT omitting generated columns works; STORED and VIRTUAL values read back computed") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          id <- items
            .insert((name = "lamp", price_net = BigDecimal(100), vat_rate = BigDecimal("0.2")))
            .returning(i => i.id)
            .compile
            .unique(s)
          row <- items.select(i => (i.price_gross, i.label)).where(i => i.id === Param.bind(id)).compile.unique(s)
          _ = assertEquals(row._1.map(_.toDouble), Some(120.0))
          _ = assertEquals(row._2, Some("LAMP"))
        } yield ()
      }
    }
  }

  test("UPDATE of an input column recomputes the generated ones; generated columns are usable on the RHS") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          id <- items
            .insert((name = "desk", price_net = BigDecimal(10), vat_rate = BigDecimal("0.5")))
            .returning(i => i.id)
            .compile
            .unique(s)
          gross <- items.update
            .set(i => i.price_net := Param.bind(BigDecimal(20)))
            .where(i => i.id === Param.bind(id))
            .returning(i => i.price_gross)
            .compile
            .unique(s)
          _ = assertEquals(gross.map(_.toDouble), Some(30.0))
        } yield ()
      }
    }
  }

  test("SchemaValidator accepts matching .withGenerated declarations") {
    withContainers { containers =>
      session(containers).use { s =>
        SchemaValidator.validate[IO](s, items).map { report =>
          assert(report.isValid, report.mismatches.map(_.pretty).mkString("; "))
        }
      }
    }
  }

  test("SchemaValidator flags generated columns that aren't declared, and declared ones that aren't generated") {
    val undeclared = Table.of[PricedItem]("priced_items").withPrimary("id").withDefault("id")
    val wrong      = items.withGenerated("name")
    withContainers { containers =>
      session(containers).use { s =>
        for {
          r1 <- SchemaValidator.validate[IO](s, undeclared)
          r2 <- SchemaValidator.validate[IO](s, wrong)
          _ = assertEquals(
            r1.mismatches.collect { case m: Mismatch.GeneratedMismatch => (m.column, m.actualGenerated) }.toSet,
            Set(("price_gross", true), ("label", true))
          )
          _ = assertEquals(
            r2.mismatches.collect { case m: Mismatch.GeneratedMismatch => (m.column, m.actualGenerated) },
            List(("name", false))
          )
        } yield ()
      }
    }
  }
}
