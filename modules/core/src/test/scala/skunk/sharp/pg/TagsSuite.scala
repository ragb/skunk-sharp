package skunk.sharp.pg

import skunk.data.Type
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.*

import java.util.UUID

object TagsSuite {
  case class Customer(id: UUID, email: Varchar[256], age: Int2, name: Bpchar[8])
}

class TagsSuite extends munit.FunSuite {
  import TagsSuite.Customer

  test("Table.of picks the tag-driven codec (Varchar[256] → varchar(256), Int2 → int2)") {
    val customers = Table.of[Customer]("customers")
    val cols      = customers.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.tpe),
      List(Type.uuid, Type.varchar(256), Type.int2, Type.bpchar(8))
    )
  }

  test("Table.builder.column[Tag] infers the tag's codec") {
    val customers = Table
      .builder("customers")
      .column[UUID]("id", primary = true)
      .column[Varchar[256]]("email", unique = true)
      .column[Int2]("age")
      .column[Bpchar[8]]("name")
      .build

    val cols = customers.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(
      cols.map(_.tpe),
      List(Type.uuid, Type.varchar(256), Type.int2, Type.bpchar(8))
    )
    val emailCol = cols.find(_.name == "email").get
    assert(emailCol.isUnique, "email column picked up unique = true")
    val idCol = cols.find(_.name == "id").get
    assert(idCol.isPrimary, "id column picked up primary = true")
  }

  test("explicit-codec column still works alongside the inferred form") {
    val customers = Table
      .builder("customers")
      .column("id", skunk.codec.all.uuid, primary = true)
      .column[Varchar[256]]("email")
      .build
    val cols = customers.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(Type.uuid, Type.varchar(256)))
  }

  test("columnOpt[Tag] wraps the codec in .opt and marks column nullable") {
    val customers = Table
      .builder("customers")
      .column[UUID]("id")
      .columnOpt[Varchar[128]]("alias")
      .build
    val cols  = customers.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val alias = cols.find(_.name == "alias").get
    assertEquals(alias.tpe, Type.varchar(128))
    assert(alias.isNullable)
  }

  test("columnDefaulted[Tag] sets the Default phantom to true") {
    val customers = Table
      .builder("customers")
      .columnDefaulted[Int8]("id", primary = true)
      .column[Varchar[256]]("email")
      .build
    val cols = customers.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val id   = cols.find(_.name == "id").get
    assertEquals(id.tpe, Type.int8)
    assert(id.hasDefault)
  }

  test("Numeric[P, S] tag resolves to numeric(p, s)") {
    val billing = Table
      .builder("billing")
      .column[Numeric[10, 2]]("amount")
      .build
    val cols = billing.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(Type.numeric(10, 2)))
  }

  test("tag values are constructed explicitly via the companion apply") {
    val c = Customer(
      id = UUID.randomUUID,
      email = Varchar[256]("hello@example.com"),
      age = Int2(42.toShort),
      name = Bpchar[8]("alice   ")
    )
    assert(c.email.startsWith("hello"), c.email)
  }

  test("Pg.lower / Pg.upper / Pg.length accept tagged string columns (Varchar[N] / Bpchar[N])") {
    val customers = Table.of[Customer]("customers")
    val af = customers
      .select(c => (Pg.lower(c.email), Pg.upper(c.name), Pg.length(c.email)))
      .compile
      .af

    assertEquals(
      af.fragment.sql,
      """SELECT lower("email"), upper("name"), length("email") FROM "customers""""
    )
  }

  test(".like / .ilike accept tagged string columns") {
    val customers = Table.of[Customer]("customers")
    val af = customers
      .select
      .where(c => c.email.like("%@example.com") && c.name.ilike("alice%"))
      .compile
      .af

    assert(af.fragment.sql.contains("""LIKE $1"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""ILIKE $2"""), af.fragment.sql)
  }
}
