package skunk.sharp.dsl

import skunk.Void
import skunk.sharp.dsl.*

import scala.compiletime.testing.*

object MergeSuite {
  case class Stock(sku: String, qty: Int, note: Option[String], qty_x2: Int)
  case class Incoming(sku: String, qty: Int)

  val stock    = Table.of[Stock]("stock").withPrimary("sku").withDefault("note").withGenerated("qty_x2")
  val incoming = Table.of[Incoming]("incoming")
}

class MergeSuite extends munit.FunSuite {
  import MergeSuite.*

  private inline def errorsOf(inline code: String): String = {
    val errs = typeCheckErrors(code)
    assert(errs.nonEmpty, "expected a compile error")
    errs.map(_.message).mkString("\n")
  }

  private val head = """MERGE INTO "stock" USING "incoming" ON "stock"."sku" = "incoming"."sku""""

  test("upsert: WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT") {
    val q: CommandTemplate[Void] = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenMatched
      .update(r => r.stock.qty := r.incoming.qty)
      .whenNotMatched
      .insert(s => (sku = s.sku, qty = s.qty))
      .compile
    assertEquals(
      q.fragment.sql,
      head + """ WHEN MATCHED THEN UPDATE SET "qty" = "incoming"."qty"""" +
        """ WHEN NOT MATCHED THEN INSERT ("sku", "qty") VALUES ("incoming"."sku", "incoming"."qty")"""
    )
  }

  test("conditional branches, DELETE and DO NOTHING; branch order is kept") {
    val q = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenMatched(r => r.incoming.qty === 0)
      .delete
      .whenMatched(r => r.stock.qty === r.incoming.qty)
      .doNothing
      .whenMatched
      .update(r => (r.stock.qty := r.incoming.qty, r.stock.note := Pg.nullOf[String]))
      .whenNotMatched(s => s.qty > 0)
      .insert(s => (sku = s.sku, qty = s.qty))
      .whenNotMatched
      .doNothing
      .compile
    assertEquals(
      q.fragment.sql,
      head +
        """ WHEN MATCHED AND "incoming"."qty" = 0 THEN DELETE""" +
        """ WHEN MATCHED AND "stock"."qty" = "incoming"."qty" THEN DO NOTHING""" +
        """ WHEN MATCHED THEN UPDATE SET "qty" = "incoming"."qty", "note" = NULL""" +
        """ WHEN NOT MATCHED AND "incoming"."qty" > 0 THEN INSERT ("sku", "qty") VALUES ("incoming"."sku", "incoming"."qty")""" +
        """ WHEN NOT MATCHED THEN DO NOTHING"""
    )
  }

  test("WHEN NOT MATCHED BY SOURCE sees only the target") {
    val q = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenNotMatchedBySource(t => t.qty === 0)
      .delete
      .whenNotMatchedBySource
      .update(t => t.qty := 0)
      .compile
    assertEquals(
      q.fragment.sql,
      head +
        """ WHEN NOT MATCHED BY SOURCE AND "stock"."qty" = 0 THEN DELETE""" +
        """ WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "qty" = 0"""
    )
  }

  test("typed Params thread through ON, conditions and actions in SQL order") {
    val q: CommandTemplate[(String, Int, Int)] = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku && (r.stock.sku !== Param[String]))
      .whenMatched(r => r.incoming.qty > Param[Int])
      .update(r => r.stock.qty := r.incoming.qty)
      .whenNotMatched
      .insert(s => (sku = s.sku, qty = Param[Int]))
      .compile
    assertEquals(
      q.fragment.sql,
      """MERGE INTO "stock" USING "incoming" ON ("stock"."sku" = "incoming"."sku" AND "stock"."sku" <> $1)""" +
        """ WHEN MATCHED AND "incoming"."qty" > $2 THEN UPDATE SET "qty" = "incoming"."qty"""" +
        """ WHEN NOT MATCHED THEN INSERT ("sku", "qty") VALUES ("incoming"."sku", $3)"""
    )
  }

  test("a typed subquery source threads its Params first") {
    val q: CommandTemplate[(Int, Int)] = stock
      .merge(incoming.select.where(i => i.qty > Param[Int]).alias("src"))
      .on(r => r.stock.sku === r.src.sku)
      .whenMatched
      .update(r => r.stock.qty := r.src.qty)
      .whenNotMatched(s => s.qty < Param[Int])
      .doNothing
      .compile
    assertEquals(
      q.fragment.sql,
      """MERGE INTO "stock" USING (SELECT "sku", "qty" FROM "incoming" WHERE "qty" > $1) AS "src"""" +
        """ ON "stock"."sku" = "src"."sku"""" +
        """ WHEN MATCHED THEN UPDATE SET "qty" = "src"."qty"""" +
        """ WHEN NOT MATCHED AND "src"."qty" < $2 THEN DO NOTHING"""
    )
  }

  test("an aliased source table uses its alias") {
    val q = stock
      .merge(incoming.alias("i"))
      .on(r => r.stock.sku === r.i.sku)
      .whenMatched
      .delete
      .compile
    assertEquals(
      q.fragment.sql,
      """MERGE INTO "stock" USING "incoming" AS "i" ON "stock"."sku" = "i"."sku" WHEN MATCHED THEN DELETE"""
    )
  }

  test("RETURNING merge_action() and columns from both sides") {
    val q = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenMatched
      .update(r => r.stock.qty := r.incoming.qty)
      .whenNotMatched
      .insert(s => (sku = s.sku, qty = s.qty))
      .returningTuple(r => (Pg.mergeAction, r.stock.sku, r.stock.qty))
    val _: QueryTemplate[Void, (String, String, Int)] = q
    assert(q.fragment.sql.endsWith(""" RETURNING merge_action(), "stock"."sku", "stock"."qty""""), q.fragment.sql)
  }

  // ---- Compile-time rejections ----

  test(".compile without any WHEN branch does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).compile
    """)
    assert(msg.contains("at least one WHEN branch"), msg)
  }

  test("WHEN NOT MATCHED can't see the target") {
    errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatched(s => s.note.isNull)
    """)
  }

  test("WHEN NOT MATCHED BY SOURCE can't see the source") {
    errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatchedBySource(t => t.sku === t.sku).delete
        .whenNotMatchedBySource(r => r.incoming.qty === 0)
    """)
  }

  test("INSERT must cover required target columns") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatched.insert(s => (sku = s.sku))
    """)
    assert(msg.contains("missing required column \"qty\""), msg)
  }

  test("INSERT rejects unknown columns, generated columns and mistyped values") {
    val unknown = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatched
        .insert(s => (sku = s.sku, qty = s.qty, nope = s.qty))
    """)
    assert(unknown.contains("\"nope\""), unknown)
    val generated = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatched
        .insert(s => (sku = s.sku, qty = s.qty, qty_x2 = s.qty))
    """)
    assert(generated.contains("\"qty_x2\" is generated"), generated)
    val mistyped = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenNotMatched
        .insert(s => (sku = s.qty, qty = s.qty))
    """)
    assert(
      mistyped.contains("""MERGE INSERT value for column ("sku" : String) doesn't match the column's type String"""),
      mistyped
    )
  }

  test("WHEN MATCHED UPDATE can't assign a generated column") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenMatched.update(r => r.stock.qty_x2 := r.incoming.qty)
    """)
    assert(msg.contains("\"qty_x2\" is generated"), msg)
  }

  test("MERGE is not available on a view, and a source alias can't clash with the target") {
    errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      View.of[Incoming]("v").merge(incoming)
    """)
    errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming.alias("stock"))
    """)
  }

  test("a multi-array unnest source: the whole batch is typed Args of one statement") {
    val q: CommandTemplate[(List[String], List[Int])] = stock
      .merge(Pg.unnestAsRelation((sku = Param[List[String]], qty = Param[List[Int]])).alias("batch"))
      .on(r => r.stock.sku === r.batch.sku)
      .whenMatched
      .update(r => r.stock.qty := r.batch.qty)
      .whenNotMatched
      .insert(b => (sku = b.sku, qty = b.qty))
      .compile
    assertEquals(
      q.fragment.sql,
      """MERGE INTO "stock" USING unnest($1, $2) AS "batch"("sku", "qty") ON "stock"."sku" = "batch"."sku"""" +
        """ WHEN MATCHED THEN UPDATE SET "qty" = "batch"."qty"""" +
        """ WHEN NOT MATCHED THEN INSERT ("sku", "qty") VALUES ("batch"."sku", "batch"."qty")"""
    )
  }

  test("tuple .update folds Params into the MERGE Args (matched and by-source)") {
    val q: CommandTemplate[(Int, Int)] = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenMatched
      .update(r => (r.stock.qty := Param[Int], r.stock.note := Pg.nullOf[String]))
      .whenNotMatchedBySource
      .update(t => (t.qty := Param[Int], t.note := Pg.nullOf[String]))
      .compile
    assertEquals(
      q.fragment.sql,
      head + """ WHEN MATCHED THEN UPDATE SET "qty" = $1, "note" = NULL""" +
        """ WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "qty" = $2, "note" = NULL"""
    )
    assertEquals(q.fragment.encoder.encode((5, 6)).flatten.map(_.value), List("5", "6"))
  }

  test("WHEN MATCHED UPDATE can't assign a source column") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku).whenMatched.update(r => r.incoming.qty := r.stock.qty)
    """)
    assert(msg.contains("\"qty\" belongs to the MERGE source"), msg)
  }

  test("a branch after an unconditional branch of the same kind is unreachable — compile error") {
    val m1 = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku)
        .whenMatched.delete
        .whenMatched(r => r.incoming.qty === 0).doNothing
    """)
    assert(m1.contains("unreachable WHEN MATCHED branch"), m1)
    val m2 = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku)
        .whenNotMatched.doNothing
        .whenNotMatched.insert(s => (sku = s.sku, qty = s.qty))
    """)
    assert(m2.contains("unreachable WHEN NOT MATCHED branch"), m2)
    val m3 = errorsOf("""
      import skunk.sharp.dsl.*
      import MergeSuite.*
      stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku)
        .whenNotMatchedBySource.delete
        .whenNotMatchedBySource(t => t.qty === 0).doNothing
    """)
    assert(m3.contains("unreachable WHEN NOT MATCHED BY SOURCE branch"), m3)
  }

  test("conditional branches before an unconditional one, and other kinds after it, are fine") {
    val q = stock
      .merge(incoming)
      .on(r => r.stock.sku === r.incoming.sku)
      .whenMatched(r => r.incoming.qty === 0)
      .delete
      .whenMatched(r => r.incoming.qty === 1)
      .doNothing
      .whenMatched
      .update(r => r.stock.qty := r.incoming.qty)
      .whenNotMatched
      .insert(s => (sku = s.sku, qty = s.qty))
      .whenNotMatchedBySource
      .delete
      .compile
    assert(q.fragment.sql.endsWith("WHEN NOT MATCHED BY SOURCE THEN DELETE"), q.fragment.sql)
  }
}
