package skunk.sharp.named

import skunk.sharp.NamedArgs
import skunk.sharp.dsl.{*, given}

import java.util.UUID
import scala.compiletime.testing.*

object NamedParamSuite {
  case class Room(id: UUID, building_id: UUID, name: String, capacity: Int)
  case class Sync(name: String, capacity: Int)
  val rooms = Table.of[Room]("rooms").withPrimary("id")

  /** The execute-time argument type of a template, as a function input — assignable only if the shapes agree. */
  def runArgsOf[A, R](q: QueryTemplate[A, R]): NamedArgs.RunArgs[A] => Unit = _ => ()
  def cmdArgsOf[A](c: CommandTemplate[A]): NamedArgs.RunArgs[A] => Unit     = _ => ()
}

/** Deliberately outside `skunk.sharp.dsl`: named params are used from user code, where `private[dsl]` isn't visible. */
class NamedParamSuite extends munit.FunSuite {
  import NamedParamSuite.*

  private val bid = UUID.fromString("00000000-0000-0000-0000-00000000000b")

  private def encoded(af: skunk.AppliedFragment): List[String] =
    af.fragment.encoder.encode(af.argument).flatten.map(_.value)

  private def byBuildingAndMin =
    rooms.select
      .where(r =>
        r.building_id === Param.named["buildingId", UUID] &&
          r.capacity >= Param.named["min", Int] &&
          (r.id !== Param.named["buildingId", UUID])
      )
      .compile

  test("all-named statement runs with a named tuple; a repeated name is passed once") {
    val q = byBuildingAndMin
    assertEquals(
      q.fragment.sql,
      """SELECT "id", "building_id", "name", "capacity" FROM "rooms" WHERE (("building_id" = $1 AND "capacity" >= $2) AND "id" <> $3)"""
    )
    assertEquals(encoded(q.bind((buildingId = bid, min = 4))), List(bid.toString, "4", bid.toString))
  }

  test("named Args never collapse to Void: the run type is exactly the named tuple, every placeholder is encoded") {
    val q                                         = byBuildingAndMin
    val f: ((buildingId: UUID, min: Int)) => Unit = runArgsOf(q)
    assertEquals(q.fragment.encoder.types.size, 3)
  }

  test("fields follow the order of each name's first appearance in the SQL") {
    val q =
      rooms.select.where(r => r.building_id === Param.named["b", UUID] && r.capacity >= Param.named["min", Int]).compile
    val _: ((b: UUID, min: Int)) => Unit = runArgsOf(q)
    assertEquals(encoded(q.bind((b = bid, min = 2))), List(bid.toString, "2"))
  }

  test("a single named param runs with a one-field named tuple") {
    val q                       = rooms.delete.where(r => r.id === Param.named["id", UUID]).compile
    val _: ((id: UUID)) => Unit = cmdArgsOf(q)
    assertEquals(encoded(q.bind((id = bid))), List(bid.toString))
  }

  test("mixing named and positional params keeps positional Args; named slots take a bare value") {
    val q = rooms.select.where(r => r.building_id === Param.named["b", UUID] && r.capacity >= Param[Int]).compile
    val _: ((UUID, Int)) => Unit = runArgsOf(q)
    assertEquals(encoded(q.bind((bid, 3))), List(bid.toString, "3"))
  }

  test("statements without named params are unchanged") {
    val q: QueryTemplate[(UUID, Int), Room] =
      rooms.select.where(r => r.building_id === Param[UUID] && r.capacity >= Param[Int]).compile.to[Room]
    assertEquals(encoded(q.bind((bid, 3))), List(bid.toString, "3"))
  }

  test("named params in commands: tuple UPDATE SET + WHERE") {
    val upd = rooms.update
      .set(r => (r.capacity := Param.named["capacity", Int], r.name := Param.named["name", String]))
      .where(r => r.id === Param.named["id", UUID])
      .compile
    val _: ((capacity: Int, name: String, id: UUID)) => Unit = cmdArgsOf(upd)
    assertEquals(encoded(upd.bind((capacity = 5, name = "A", id = bid))), List("5", "A", bid.toString))
  }

  test("the same name used with two different types does not compile") {
    val msg = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      import NamedParamSuite.rooms
      val q = rooms.select.where(r => r.building_id === Param.named["x", UUID] && r.capacity >= Param.named["x", Int]).compile
      q.bind((x = UUID.randomUUID))
    """).map(_.message).mkString("\n")
    assert(msg.contains("""named parameter ("x" : String) is used with different types"""), msg)
  }

  test("a misspelled or missing name does not compile") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      import NamedParamSuite.rooms
      val q = rooms.delete.where(r => r.id === Param.named["id", UUID]).compile
      q.bind((idd = UUID.randomUUID))
    """)
    assert(errs.nonEmpty)
  }

  test("named params allocate no dynamic AppliedFragments (compile + bind, repeated)") {
    val counter = skunk.sharp.internal.RawConstants.rawDynamicThreadCount
    def build() = {
      val q = byBuildingAndMin
      q.bind((buildingId = bid, min = 1))
    }
    build()
    val before = counter.get
    (1 to 50).foreach(_ => build())
    assertEquals(counter.get - before, 0L)
  }

  test("a fully named MERGE: labelled unnestRows batch + a named id used twice") {
    val q = rooms
      .merge(Pg.unnestRows[Sync]("rooms").alias("incoming"))
      .on(r => r.rooms.building_id === Param.named["buildingId", UUID] && r.rooms.name === r.incoming.name)
      .whenMatched
      .update(r => r.rooms.capacity := r.incoming.capacity)
      .whenNotMatchedBySource(t => t.building_id === Param.named["buildingId", UUID])
      .delete
      .compile
    val _: ((rooms: List[Sync], buildingId: UUID)) => Unit = cmdArgsOf(q)
    assertEquals(
      encoded(q.bind((rooms = List(Sync("A", 2), Sync("B", 3)), buildingId = bid))),
      List("{\"A\",\"B\"}", "{\"2\",\"3\"}", bid.toString, bid.toString)
    )
  }
}
