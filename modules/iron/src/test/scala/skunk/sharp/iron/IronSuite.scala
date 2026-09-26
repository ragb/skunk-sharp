package skunk.sharp.iron

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.all.*
import skunk.data.Type
import skunk.sharp.*

object IronSuite {
  type Email = String :| Match["^[^@]+@[^@]+$"]
  type Age   = Int :| Positive

  case class Person(id: Int, email: Email, age: Age)

  // Bridge cases: Iron constraints that have a DB-type counterpart.
  type Name   = String :| MaxLength[64]
  type Postal = String :| FixedLength[5]
  case class Party(name: Name, postalCode: Postal)

  type Money = BigDecimal :| Precision[10, 2]
  case class Invoice(id: Int, total: Money)
}

class IronSuite extends munit.FunSuite {
  import IronSuite.*

  test("Iron refinements participate in Table.of derivation") {
    val people = Table.of[Person]("people").withPrimary("id")

    val cols = people.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "age"))
    assertEquals(cols.map(_.tpe), List(Type.int4, Type.text, Type.int4))
  }

  test("Iron-based columns participate in the DSL") {
    val people = Table.of[Person]("people")
    val cv     = ColumnsView(people.columns)

    val emailCol: TypedColumn[Email, false, "email"] = cv.email
    assertEquals(emailCol.name, "email")
  }

  test("Iron bridge: String :| MaxLength[N] picks varchar(n)") {
    val parties = Table.of[Party]("parties")
    val cols    = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val name    = cols.find(_.name == "name").get
    assertEquals(name.tpe, Type.varchar(64))
  }

  test("Iron bridge: String :| FixedLength[N] picks bpchar(n)") {
    val parties = Table.of[Party]("parties")
    val cols    = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val postal  = cols.find(_.name == "postalCode").get
    assertEquals(postal.tpe, Type.bpchar(5))
  }

  test("BigDecimal :| Precision[P, S] maps to numeric(p, s)") {
    val cols = Table.of[Invoice]("invoices").columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.find(_.name == "total").map(_.tpe.name), Some("numeric(10,2)"))
  }

  test("Precision[P, S]: integer digits <= P - S, scale <= S; trailing zeros and sub-1 values handled") {
    assert(Precision.fits(BigDecimal("12345678.99"), 10, 2))
    assert(Precision.fits(BigDecimal("0.5"), 2, 2))    // no integer digits
    assert(Precision.fits(BigDecimal("1.2300"), 3, 2)) // trailing zeros don't count
    assert(Precision.fits(BigDecimal("0"), 2, 2))
    assert(!Precision.fits(BigDecimal("123456789.99"), 10, 2)) // 9 integer digits > 8
    assert(!Precision.fits(BigDecimal("1.234"), 10, 2))        // scale 3 > 2 — rejected, not rounded
    assert(!Precision.fits(BigDecimal("1"), 2, 2))             // numeric(2,2) holds only |v| < 1
  }

  test("refineEither[Precision[P, S]] gives a Left with the constraint's message") {
    assertEquals(BigDecimal("19.99").refineEither[Precision[10, 2]].map(_.toString), Right("19.99"))
    assertEquals(BigDecimal("19.999").refineEither[Precision[10, 2]], Left("Should fit numeric(10, 2)"))
  }
}
