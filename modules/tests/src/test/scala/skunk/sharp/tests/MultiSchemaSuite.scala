/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package skunk.sharp.tests

import cats.effect.IO
import dumbo.{ConnectionConfig, Dumbo}
import dumbo.logging.Implicits.console
import org.typelevel.otel4s.metrics.Meter.Implicits.given
import org.typelevel.otel4s.trace.Tracer.Implicits.given
import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.*

import java.time.OffsetDateTime
import java.util.UUID

object MultiSchemaSuite {

  // Tables live in non-public schemas: `app.products` and `audit.events`. Both case classes mirror the migration
  // column shapes including `Varchar[N]` / `Numeric[P, S]` tags so the validator has tight expected types to diff.
  case class Product(id: UUID, name: Varchar[256], price: Numeric[10, 2])

  case class Event(id: Long, action: Varchar[64], product_id: Option[UUID], occurred_at: OffsetDateTime)
}

/**
 * Exercises relations declared in non-`public` Postgres schemas — confirms that
 *   - the builder renders `"app"."products"` etc. in generated SQL,
 *   - cross-schema JOINs work end-to-end,
 *   - the [[SchemaValidator]] scopes its `information_schema` queries by declared schema.
 *
 * Uses a dedicated migrations directory (`migrations-multischema`) so it doesn't fight the default-public suites
 * sharing the main `migrations` folder.
 */
class MultiSchemaSuite extends PgFixture {
  import MultiSchemaSuite.*

  override protected def runMigrations(conn: ConnectionConfig): IO[Unit] =
    Dumbo.withResourcesIn[IO]("migrations-multischema").apply(conn).runMigration.void

  private val products = Table.of[Product]("products").inSchema("app").withPrimary("id")

  private val events =
    Table.of[Event]("events").inSchema("audit").withPrimary("id").withDefault("id").withDefault("occurred_at")

  test("insert + select round-trip on a schema-qualified table (app.products)") {
    withContainers { containers =>
      session(containers).use { s =>
        val id = UUID.randomUUID
        for {
          _ <- products
            .insert((id = id, name = Varchar[256]("widget"), price = Numeric[10, 2](BigDecimal("9.99"))))
            .compile.run(s)
          _ <- assertIO(
            products.select(p => (p.name, p.price)).where(p => p.id === id).compile.unique(s),
            (Varchar[256]("widget"), Numeric[10, 2](BigDecimal("9.99")))
          )
        } yield ()
      }
    }
  }

  test("cross-schema JOIN: audit.events LEFT JOIN app.products") {
    withContainers { containers =>
      session(containers).use { s =>
        val pid = UUID.randomUUID
        for {
          _ <- products
            .insert((id = pid, name = Varchar[256]("gadget"), price = Numeric[10, 2](BigDecimal("1.00"))))
            .compile.run(s)
          _ <- events.insert.values(
            (action = Varchar[64]("view"), product_id = Some(pid)),
            (action = Varchar[64]("heartbeat"), product_id = Option.empty[UUID])
          ).compile.run(s)
          // LEFT JOIN: all events back, with product name when the event has a product_id.
          rows <- events
            .leftJoin(products)
            // events.product_id is nullable; r.products.id is not. Cast the nullable side to drop the Option from
            // the Scala type — SQL NULL still flows correctly through the = operator in an ON predicate.
            .on(r => r.events.product_id.cast[UUID] ==== r.products.id)
            .select(r => (r.events.action, r.products.name))
            .orderBy(r => r.events.action.asc)
            .compile.run(s)
          // Widen Varchar-tag results to plain String for comparison; equivalence is via subtype erasure.
          _ = assertEquals(
            rows.map((a, n) => (a: String, n.map(x => x: String))),
            List(("heartbeat", Option.empty[String]), ("view", Some("gadget")))
          )
        } yield ()
      }
    }
  }

  test("SchemaValidator passes on both schemas") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          report <- SchemaValidator.validate[cats.effect.IO](s, products, events)
          _ = assert(report.isValid, s"unexpected mismatches: ${report.mismatches}")
        } yield ()
      }
    }
  }

  test("SchemaValidator catches a wrong-schema declaration (audit.products does not exist)") {
    withContainers { containers =>
      session(containers).use { s =>
        // Same columns as the real `app.products`, but pointed at `audit` — so the table doesn't exist there.
        val misplaced = Table.of[Product]("products").inSchema("audit")
        for {
          report <- SchemaValidator.validate[cats.effect.IO](s, misplaced)
          _ = assert(
            report.mismatches.exists {
              case Mismatch.RelationMissing(rel, _) => rel.contains("audit") && rel.contains("products")
              case _                                => false
            },
            s"expected a RelationMissing on audit.products, got ${report.mismatches}"
          )
        } yield ()
      }
    }
  }
}
