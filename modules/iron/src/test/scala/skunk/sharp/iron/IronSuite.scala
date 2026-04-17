package skunk.sharp.iron

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.all.*
import skunk.data.Type
import skunk.sharp.*

object IronSuite {
  type Email = String :| Match["^[^@]+@[^@]+$"]
  type Age   = Int :| Positive

  case class Person(id: Int, email: Email, age: Age)
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

    val emailCol: TypedColumn[Email, false] = cv.email
    assertEquals(emailCol.name, "email")
  }
}
