package skunk.sharp.tests

import cats.effect.IO
import cats.syntax.all.*
import skunk.Session
import skunk.sharp.dsl.*

object MergeSuite {
  case class Stock(sku: String, qty: Int, note: Option[String], qty_x2: Option[Int])
  case class Incoming(sku: String, qty: Int)
}

/** MERGE end-to-end on PG 18 (`WHEN NOT MATCHED BY SOURCE` / `RETURNING` need 17+). Matches V14__merge.sql. */
class MergeSuite extends PgFixture {
  import MergeSuite.*

  private val stock    = Table.of[Stock]("merge_stock").withPrimary("sku").withDefault("note").withGenerated("qty_x2")
  private val incoming = Table.of[Incoming]("merge_incoming").withPrimary("sku")

  /** Reset both tables to: stock {a:1, b:2, c:3}, incoming {a:10, b:0, d:4}. */
  private def seed(s: Session[IO]): IO[Unit] =
    for {
      _ <- stock.delete.deleteAll.compile.run(s)
      _ <- incoming.delete.deleteAll.compile.run(s)
      _ <- List("a" -> 1, "b" -> 2, "c" -> 3).traverse_ { case (k, q) =>
        stock.insert((sku = k, qty = q)).compile.run(s)
      }
      _ <- List("a" -> 10, "b" -> 0, "d" -> 4).traverse_ { case (k, q) =>
        incoming.insert((sku = k, qty = q)).compile.run(s)
      }
    } yield ()

  private def stockRows(s: Session[IO]): IO[List[(String, Int, Option[Int])]] =
    stock.select(t => (t.sku, t.qty, t.qty_x2)).orderBy(t => t.sku.asc).compile.run(s)

  test("sync: update matches, delete zeroed matches, insert new, delete rows missing from the source") {
    withContainers { containers =>
      session(containers).use { s =>
        val sync = stock
          .merge(incoming)
          .on(r => r.merge_stock.sku === r.merge_incoming.sku)
          .whenMatched(r => r.merge_incoming.qty === 0)
          .delete
          .whenMatched
          .update(r => r.merge_stock.qty := r.merge_incoming.qty)
          .whenNotMatched
          .insert(i => (sku = i.sku, qty = i.qty))
          .whenNotMatchedBySource
          .delete
          .compile
        for {
          _    <- seed(s)
          _    <- sync.run(s)
          rows <- stockRows(s)
          // a updated (generated column recomputed), b deleted (qty 0), c deleted (not in source), d inserted.
          _ = assertEquals(rows, List(("a", 10, Some(20)), ("d", 4, Some(8))))
        } yield ()
      }
    }
  }

  test("RETURNING merge_action() reports what each branch did") {
    withContainers { containers =>
      session(containers).use { s =>
        val q = stock
          .merge(incoming)
          .on(r => r.merge_stock.sku === r.merge_incoming.sku)
          .whenMatched(r => r.merge_incoming.qty === 0)
          .delete
          .whenMatched
          .update(r => r.merge_stock.qty := r.merge_incoming.qty)
          .whenNotMatched
          .insert(i => (sku = i.sku, qty = i.qty))
          .whenNotMatchedBySource
          .update(t => t.qty := 0)
          .returningTuple(r => (Pg.mergeAction, r.merge_stock.sku))
        for {
          _    <- seed(s)
          rows <- q.run(s)
          _ = assertEquals(
            rows.sortBy(_._2),
            List(("UPDATE", "a"), ("DELETE", "b"), ("UPDATE", "c"), ("INSERT", "d"))
          )
        } yield ()
      }
    }
  }

  test("typed Params in a subquery source and a branch condition bind at execute time") {
    withContainers { containers =>
      session(containers).use { s =>
        // Only merge source rows with qty >= $1; only insert when qty < $2.
        val q: CommandTemplate[(Int, Int)] = stock
          .merge(incoming.select.where(i => i.qty >= Param[Int]).alias("src"))
          .on(r => r.merge_stock.sku === r.src.sku)
          .whenMatched
          .update(r => r.merge_stock.qty := r.src.qty)
          .whenNotMatched(i => i.qty < Param[Int])
          .insert(i => (sku = i.sku, qty = i.qty))
          .compile
        for {
          _    <- seed(s)
          _    <- q.run(s)((1, 5))
          rows <- stockRows(s)
          // b (qty 0) filtered out by the source; a updated; d (4 < 5) inserted; c untouched.
          _ = assertEquals(rows.map(r => (r._1, r._2)), List("a" -> 10, "b" -> 2, "c" -> 3, "d" -> 4))
        } yield ()
      }
    }
  }

  test("DO NOTHING branches leave rows alone") {
    withContainers { containers =>
      session(containers).use { s =>
        val q = stock
          .merge(incoming)
          .on(r => r.merge_stock.sku === r.merge_incoming.sku)
          .whenMatched
          .doNothing
          .whenNotMatched
          .doNothing
          .compile
        for {
          _    <- seed(s)
          _    <- q.run(s)
          rows <- stockRows(s)
          _ = assertEquals(rows.map(_._1), List("a", "b", "c"))
        } yield ()
      }
    }
  }

  // The core MergeSuite lives in `skunk.sharp.dsl`, where `private[dsl]` is visible; inline MERGE methods expand into
  // user code, so exercise every one of them from outside the package too.
  test("every MERGE builder method compiles outside the dsl package") {
    val q = stock
      .merge(incoming)
      .on(r => r.merge_stock.sku === r.merge_incoming.sku)
      .whenMatched(r => r.merge_incoming.qty > 100)
      .update(r => (r.merge_stock.qty := r.merge_incoming.qty, r.merge_stock.note := Pg.nullOf[String]))
      .whenMatched(r => r.merge_incoming.qty > 50)
      .doNothing
      .whenNotMatched(i => i.qty > 0)
      .doNothing
      .whenNotMatchedBySource(t => t.qty > 10)
      .update(t => (t.qty := 0, t.note := Pg.nullOf[String]))
      .whenNotMatchedBySource(t => t.qty > 5)
      .doNothing
      .whenNotMatchedBySource(t => t.qty > 1)
      .delete
      .returning(r => r.merge_stock.qty)
    assert(q.fragment.sql.startsWith("MERGE INTO \"merge_stock\" USING \"merge_incoming\""), q.fragment.sql)
    assert(q.fragment.sql.endsWith(" RETURNING \"merge_stock\".\"qty\""), q.fragment.sql)
  }
}
