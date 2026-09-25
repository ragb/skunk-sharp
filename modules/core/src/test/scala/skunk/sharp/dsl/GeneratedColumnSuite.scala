package skunk.sharp.dsl

import skunk.sharp.dsl.*

import scala.compiletime.testing.*

object GeneratedColumnSuite {
  case class Product(id: Int, name: String, price_net: BigDecimal, vat_rate: BigDecimal, price_gross: BigDecimal)
  case class Order(id: Int, product_id: Int, total: BigDecimal)

  val products = Table.of[Product]("products").withPrimary("id").withDefault("id").withGenerated("price_gross")
  val orders   = Table.of[Order]("orders")
}

class GeneratedColumnSuite extends munit.FunSuite {
  import GeneratedColumnSuite.*

  private inline def errorsOf(inline code: String): String = {
    val errs = typeCheckErrors(code)
    assert(errs.nonEmpty, "expected a compile error")
    errs.map(_.message).mkString("\n")
  }

  // ---- Reading is unrestricted ----

  test("withGenerated flags the column at runtime") {
    val cols = products.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]]
    assertEquals(cols.filter(_.isGenerated).map(_.name), List("price_gross"))
  }

  test("generated columns are selectable and usable in WHERE") {
    val q = products.select(p => (p.name, p.price_gross)).where(p => p.price_gross > Param[BigDecimal]).compile
    assertEquals(q.fragment.sql, """SELECT "name", "price_gross" FROM "products" WHERE "price_gross" > $1""")
  }

  test("INSERT may omit a generated column") {
    val af = products.insert((name = "x", price_net = BigDecimal(10), vat_rate = BigDecimal("0.2"))).compile.af
    assertEquals(
      af.fragment.sql,
      """INSERT INTO "products" ("name", "price_net", "vat_rate") VALUES ($1, $2, $3)"""
    )
  }

  test("a generated column can be read on the right-hand side of a SET") {
    val af = products.update.set(p => p.price_net := p.price_gross).updateAll.compile.af
    assertEquals(af.fragment.sql, """UPDATE "products" SET "price_net" = "price_gross"""")
  }

  test("RETURNING a generated column is allowed") {
    val af = products
      .insert((name = "x", price_net = BigDecimal(10), vat_rate = BigDecimal("0.2")))
      .returning(p => p.price_gross)
      .compile
      .af
    assert(af.fragment.sql.endsWith(""" RETURNING "price_gross""""), af.fragment.sql)
  }

  // ---- Writing is a compile error ----

  private val generatedMsg = "\"price_gross\" is generated"

  test("withGenerated rejects unknown column names") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.Product
      Table.of[Product]("products").withGenerated("nope")
    """)
    assert(msg.contains("\"nope\""), msg)
  }

  test("INSERT with a generated column in the named-tuple row does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      type Row = (name: String, price_net: BigDecimal, vat_rate: BigDecimal, price_gross: BigDecimal)
      val row: Row = (name = "x", price_net = BigDecimal(1), vat_rate = BigDecimal(0), price_gross = BigDecimal(1))
      products.insert(row)
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("INSERT of the full case class (which includes the generated column) does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.{products, Product}
      products.insert(Product(1, "x", BigDecimal(1), BigDecimal(0), BigDecimal(1)))
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("batch INSERT with a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      type Row = (name: String, price_net: BigDecimal, vat_rate: BigDecimal, price_gross: BigDecimal)
      val row: Row = (name = "x", price_net = BigDecimal(1), vat_rate = BigDecimal(0), price_gross = BigDecimal(1))
      products.insert.values(cats.data.NonEmptyList.of(row))
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("UPDATE .set assigning a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      products.update.set(p => p.price_gross := p.price_net)
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("UPDATE tuple .set assigning a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      products.update.set(p => (p.name := "y", p.price_gross := p.price_net))
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("UPDATE .patch with a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      val p: (price_gross: Option[BigDecimal]) = (price_gross = Some(BigDecimal(1)))
      products.update.patch(p)
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("UPDATE … FROM assigning a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.{orders, products}
      products.update.from(orders).set(r => r.products.price_gross := r.orders.total)
    """)
    assert(msg.contains(generatedMsg), msg)
  }

  test("ON CONFLICT DO UPDATE assigning a generated column does not compile") {
    val msg = errorsOf("""
      import skunk.sharp.dsl.*
      import GeneratedColumnSuite.products
      products
        .insert((name = "x", price_net = BigDecimal(1), vat_rate = BigDecimal(0)))
        .onConflict(p => p.id)
        .doUpdateFromExcluded((t, ex) => t.price_gross := ex.price_gross)
    """)
    assert(msg.contains(generatedMsg), msg)
  }
}
