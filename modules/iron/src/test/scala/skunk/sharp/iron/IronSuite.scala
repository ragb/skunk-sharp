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
}

class IronSuite extends munit.FunSuite {
  import IronSuite.*

  test("Iron refinements participate in Table.of derivation") {
    val people = Table.of[Person]("people").withPrimary("id")

    val cols = people.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?, ?, ?]]]
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
    val cols    = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?, ?, ?]]]
    val name    = cols.find(_.name == "name").get
    assertEquals(name.tpe, Type.varchar(64))
  }

  test("Iron bridge: String :| FixedLength[N] picks bpchar(n)") {
    val parties = Table.of[Party]("parties")
    val cols    = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?, ?, ?]]]
    val postal  = cols.find(_.name == "postalCode").get
    assertEquals(postal.tpe, Type.bpchar(5))
  }
}
