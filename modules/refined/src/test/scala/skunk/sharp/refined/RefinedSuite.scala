package skunk.sharp.refined

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.{MaxSize, Size}
import eu.timepit.refined.generic.Equal
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string.MatchesRegex
import skunk.data.Type
import skunk.sharp.*

object RefinedSuite {
  type Email = String Refined MatchesRegex["^[^@]+@[^@]+$"]
  type Age   = Int Refined Positive

  case class Person(id: Int, email: Email, age: Age)

  // Bridge cases: refined constraints that have a DB-type counterpart.
  type Name   = String Refined MaxSize[64]
  type Postal = String Refined Size[Equal[5]]
  case class Party(name: Name, postalCode: Postal)
}

class RefinedSuite extends munit.FunSuite {
  import RefinedSuite.*

  test("refined refinements participate in Table.of derivation") {
    val people = Table.of[Person]("people").withPrimary("id")

    val cols = people.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "age"))
    assertEquals(cols.map(_.tpe), List(Type.int4, Type.text, Type.int4))
  }

  test("refined-based columns participate in the DSL") {
    val people = Table.of[Person]("people")
    val cv     = ColumnsView(people.columns)

    val emailCol: TypedColumn[Email, false, "email"] = cv.email
    assertEquals(emailCol.name, "email")
  }

  test("refined bridge: String Refined MaxSize[N] picks varchar(n)") {
    val parties = Table
      .builder("parties")
      .column[Name]("name")
      .build

    val cols = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val name = cols.find(_.name == "name").get
    assertEquals(name.tpe, Type.varchar(64))
  }

  test("refined bridge: String Refined Size[Equal[N]] picks bpchar(n)") {
    val parties = Table
      .builder("parties")
      .column[Postal]("postalCode")
      .build

    val cols   = parties.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val postal = cols.find(_.name == "postalCode").get
    assertEquals(postal.tpe, Type.bpchar(5))
  }

  test("refined columns participate in the DSL (ColumnsView selector access)") {
    val people = Table
      .builder("people")
      .column[Int]("id")
      .column[Email]("email")
      .column[Age]("age")
      .build

    val cv = ColumnsView(people.columns)
    val emailCol: TypedColumn[Email, false, "email"] = cv.email
    assertEquals(emailCol.name, "email")
  }
}
