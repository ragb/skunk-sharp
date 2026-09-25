package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.time.LocalDate

object TableRenamedSuite {
  case class Event(id: Long, day: LocalDate, kind: String)
  case class Note(event_id: Long, body: String)

  val events = Table.of[Event]("events").withCompositePrimary[("id", "day")].withDefault("id")
  val notes  = Table.of[Note]("notes")
}

class TableRenamedSuite extends munit.FunSuite {
  import TableRenamedSuite.*

  private val jan = events.renamed("events_2026_01")

  test("renamed renders every statement against the new name") {
    assertEquals(jan.select.compile.af.fragment.sql, """SELECT "id", "day", "kind" FROM "events_2026_01"""")
    assertEquals(
      jan.insert((day = LocalDate.of(2026, 1, 5), kind = "a")).compile.af.fragment.sql,
      """INSERT INTO "events_2026_01" ("day", "kind") VALUES ($1, $2)"""
    )
    assertEquals(
      jan.update.set(e => e.kind := "b").updateAll.compile.af.fragment.sql,
      """UPDATE "events_2026_01" SET "kind" = 'b'"""
    )
    assertEquals(jan.delete.deleteAll.compile.af.fragment.sql, """DELETE FROM "events_2026_01"""")
  }

  test("renamed keeps the schema") {
    val af = events.inSchema("app").renamed("events_archive").select.compile.af
    assertEquals(af.fragment.sql, """SELECT "id", "day", "kind" FROM "app"."events_archive"""")
  }

  test("renamed keeps columns and constraints — defaults, composite PK for ON CONFLICT") {
    val cols = jan.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]]
    assertEquals(cols.filter(_.hasDefault).map(_.name), List("id"))
    assertEquals(cols.filter(_.isPrimary).map(_.name), List("id", "day"))
    val af = jan
      .insert((day = LocalDate.of(2026, 1, 5), kind = "a"))
      .onConflictComposite(e => (e.id, e.day))
      .doNothing
      .compile
      .af
    assert(af.fragment.sql.endsWith("""ON CONFLICT ("id", "day") DO NOTHING"""), af.fragment.sql)
  }

  test("the new name is the default JOIN alias") {
    val af = jan
      .innerJoin(notes)
      .on(r => r.events_2026_01.id === r.notes.event_id)
      .select(r => (r.events_2026_01.kind, r.notes.body))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT "events_2026_01"."kind", "notes"."body" FROM "events_2026_01" INNER JOIN "notes" ON "events_2026_01"."id" = "notes"."event_id""""
    )
  }

  test("the original table is unchanged") {
    assertEquals(events.name, "events")
    assertEquals(jan.name, "events_2026_01")
  }
}
