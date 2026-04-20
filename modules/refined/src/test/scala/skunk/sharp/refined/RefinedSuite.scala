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

  // Bridge cases: refined constraints that have a DB-type counterpart.
  type Name   = String Refined MaxSize[64]
  type Postal = String Refined Size[Equal[5]]
}

class RefinedSuite extends munit.FunSuite {
  import RefinedSuite.*

  // Refined's `Refined[T, P]` is an unbounded opaque alias (`opaque type Refined[T, P] = T`), which defeats Scala 3
  // match-type disjointness in `ColumnsFromMirror` — so `Table.of[T]` can't derive columns for case classes
  // containing refined fields. Users with refined should use the column-by-column `Table.builder` path, which picks
  // the right codec via the `PgTypeFor[Refined[A, P]]` givens defined in this module. (Tracked as a core follow-up:
  // make `ColumnsFromMirror` dispatch via typeclasses that tolerate unbounded opaque types.)

  test("fallback: refined refinement keeps the underlying type's codec") {
    val people = Table
      .builder("people")
      .column[Int]("id")
      .column[Email]("email")
      .column[Age]("age")
      .build

    val cols = people.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.name), List("id", "email", "age"))
    assertEquals(cols.map(_.tpe), List(Type.int4, Type.text, Type.int4))
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
