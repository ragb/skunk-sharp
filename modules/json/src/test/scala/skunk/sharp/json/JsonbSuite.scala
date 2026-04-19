package skunk.sharp.json

import io.circe.Json as CirceJson
import skunk.sharp.dsl.*

import java.util.UUID

object JsonbSuite {
  case class Document(id: UUID, body: Jsonb, metadata: Json)
}

class JsonbSuite extends munit.FunSuite {
  import JsonbSuite.*

  private val docs = Table.of[Document]("documents")

  test("Jsonb / Json tags resolve to jsonb / json codec types") {
    val cols = docs.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe), List(skunk.data.Type.uuid, skunk.data.Type.jsonb, skunk.data.Type.json))
  }

  test("get / getText — key field access") {
    val af = docs
      .select(d => (d.body.get("name"), d.body.getText("name")))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "body" -> $1, "body" ->> $2 FROM "documents""""
    )
  }

  test("at / atText — array index access") {
    val af = docs.select(d => (d.body.at(0), d.body.atText(1))).compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "body" -> 0, "body" ->> 1 FROM "documents""""
    )
  }

  test("path / pathText — walk a path") {
    val af = docs
      .select(d => (d.body.path("a", "b", "c"), d.body.pathText("x", "y")))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT "body" #> $1::text[], "body" #>> $2::text[] FROM "documents""""
    )
  }

  test("contains / containedBy — @> / <@") {
    val pattern = lit(Jsonb(CirceJson.obj("status" -> CirceJson.fromString("active"))))
    val af      = docs
      .select
      .where(d => d.body.contains(pattern))
      .compile.af
    assert(af.fragment.sql.contains("""WHERE "body" @> """), af.fragment.sql)
  }

  test("hasKey / hasAnyKey / hasAllKeys") {
    val af = docs
      .select
      .where(d =>
        d.body.hasKey("email") && d.body.hasAnyKey("phone", "mobile") && d.body.hasAllKeys("name", "status")
      )
      .compile.af
    assert(af.fragment.sql.contains("""? $1"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""?| ARRAY['phone', 'mobile']"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""?& ARRAY['name', 'status']"""), af.fragment.sql)
  }

  test("Jsonb.toJsonb — cast any TypedExpr to jsonb") {
    val af = docs.select(_ => Jsonb.toJsonb(lit("hello"))).compile.af
    assertEquals(af.fragment.sql, """SELECT to_jsonb($1) FROM "documents"""")
  }

  test("Jsonb.jsonbTypeof / jsonbArrayLength / jsonbPretty") {
    val af = docs
      .select(d => (Jsonb.jsonbTypeof(d.body), Jsonb.jsonbArrayLength(d.body), Jsonb.jsonbPretty(d.body)))
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT jsonb_typeof("body"), jsonb_array_length("body"), jsonb_pretty("body") FROM "documents""""
    )
  }

  test("Jsonb.jsonbSet — builds a jsonb_set call with a PG text path literal") {
    val newVal = lit(Jsonb(CirceJson.fromString("x")))
    val af     = docs
      .select(d => Jsonb.jsonbSet(d.body, Seq("a", "b"), newVal))
      .compile.af
    assert(af.fragment.sql.contains("""jsonb_set("body", $1::text[], $2, true)"""), af.fragment.sql)
  }

  test("Jsonb.jsonbConcat / jsonbDeleteKey") {
    val patch = lit(Jsonb(CirceJson.obj("k" -> CirceJson.fromString("v"))))
    val af    = docs
      .select(d => (Jsonb.jsonbConcat(d.body, patch), Jsonb.jsonbDeleteKey(d.body, "obsolete")))
      .compile.af
    assert(af.fragment.sql.contains(""""body" || $1"""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""body" - $2"""), af.fragment.sql)
  }
}
