package skunk.sharp

import skunk.data.{Arr, Type}
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.pg.ArrayOps.*

object ArraysSuite {
  case class Post(id: Int, tags: Arr[String], score: Int)
}

class ArraysSuite extends munit.FunSuite {
  import ArraysSuite.Post

  private val posts = Table.of[Post]("posts").withPrimary("id")

  test("PgTypeFor[Arr[String]] picks text[] (Type._text)") {
    val cols = posts.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val tags = cols.find(_.name == "tags").get
    assertEquals(tags.tpe, Type._text)
  }

  test("PgTypeFor[Arr[Int]] picks int4[]") {
    case class Scores(id: Int, bucket: Arr[Int])
    val scores = Table.of[Scores]("scores")
    val cols   = scores.columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    assertEquals(cols.find(_.name == "bucket").get.tpe, Type._int4)
  }

  test("contains (@>) renders as array containment") {
    val af = posts
      .select
      .where(p => p.tags.contains(param(Arr("scala", "pg"))))
      .compile.af

    assert(af.fragment.sql.contains("\"tags\" @> $1"), af.fragment.sql)
  }

  test("containedBy (<@) renders the inverse") {
    val af = posts
      .select
      .where(p => p.tags.containedBy(param(Arr("scala", "pg", "sql"))))
      .compile.af

    assert(af.fragment.sql.contains("\"tags\" <@ $1"), af.fragment.sql)
  }

  test("overlaps (&&) renders the shared-element operator") {
    val af = posts
      .select
      .where(p => p.tags.overlaps(param(Arr("scala"))))
      .compile.af

    assert(af.fragment.sql.contains("\"tags\" && $1"), af.fragment.sql)
  }

  test("concat (||) renders as array concatenation inside projection") {
    val af = posts.select(p => p.tags.concat(param(Arr("extra")))).compile.af
    assertEquals(af.fragment.sql, """SELECT "tags" || $1 FROM "posts"""")
  }

  test("elemOf: scalar = ANY(array)") {
    val af = posts.select.where(p => param("scala").elemOf(p.tags)).compile.af
    assert(af.fragment.sql.contains("$1 = ANY(\"tags\")"), af.fragment.sql)
  }

  test("Pg.arrayLength renders array_length(col, 1)") {
    val af = posts.select(p => Pg.arrayLength(p.tags)).compile.af
    assertEquals(af.fragment.sql, """SELECT array_length("tags", 1) FROM "posts"""")
  }

  test("Pg.cardinality renders cardinality(col)") {
    val af = posts.select(p => Pg.cardinality(p.tags)).compile.af
    assertEquals(af.fragment.sql, """SELECT cardinality("tags") FROM "posts"""")
  }

  test("Pg.arrayAppend renders array_append(a, elem)") {
    val af = posts.select(p => Pg.arrayAppend(p.tags, param("done"))).compile.af
    assertEquals(af.fragment.sql, """SELECT array_append("tags", $1) FROM "posts"""")
  }

  test("Pg.arrayPrepend renders array_prepend(elem, a)") {
    val af = posts.select(p => Pg.arrayPrepend(param("top"), p.tags)).compile.af
    assertEquals(af.fragment.sql, """SELECT array_prepend($1, "tags") FROM "posts"""")
  }

  test("Pg.arrayCat renders array_cat(a, b)") {
    val af = posts.select(p => Pg.arrayCat(p.tags, param(Arr("a", "b")))).compile.af
    assertEquals(af.fragment.sql, """SELECT array_cat("tags", $1) FROM "posts"""")
  }

  test("Pg.arrayPosition renders array_position(a, elem) with int4.opt result") {
    val af = posts.select(p => Pg.arrayPosition(p.tags, param("scala"))).compile.af
    assertEquals(af.fragment.sql, """SELECT array_position("tags", $1) FROM "posts"""")
  }

  test("Pg.arrayToString renders with optional null-string") {
    val af  = posts.select(p => Pg.arrayToString(p.tags, ", ")).compile.af
    val af2 = posts.select(p => Pg.arrayToString(p.tags, ", ", "∅")).compile.af
    assertEquals(af.fragment.sql, """SELECT array_to_string("tags", $1) FROM "posts"""")
    assertEquals(af2.fragment.sql, """SELECT array_to_string("tags", $1, $2) FROM "posts"""")
  }

  test("Pg.stringToArray renders as an input-split") {
    val af = empty.select(Pg.stringToArray(param("a,b,c"), ",")).compile.af
    assertEquals(af.fragment.sql, """SELECT string_to_array($1, $2)""")
  }

  test("Pg.arrayAgg aggregates rows into an array") {
    val af = posts.select(p => Pg.arrayAgg(p.score)).compile.af
    assertEquals(af.fragment.sql, """SELECT array_agg("score") FROM "posts"""")
  }

  test("arr.to[F] / F[T].toArr conversions bridge to any cats Alternative / Foldable collection") {
    import cats.data.Chain

    val arr: Arr[String] = Arr("a", "b", "c")

    val list: List[String]     = arr.to[List]
    val vector: Vector[String] = arr.to[Vector]
    val chain: Chain[String]   = arr.to[Chain]

    assertEquals(list, List("a", "b", "c"))
    assertEquals(vector, Vector("a", "b", "c"))
    assertEquals(chain.toList, List("a", "b", "c"))

    val back: Arr[String] = list.toArr
    assertEquals(back.flattenTo(List), List("a", "b", "c"))
  }

  test("generic collPgTypeFor via cats Alternative + Foldable — List / Vector / Chain") {
    import cats.data.Chain

    case class WithList(id: Int, xs: List[Int])
    case class WithVector(id: Int, xs: Vector[Int])
    case class WithChain(id: Int, xs: Chain[Int])

    val lt = Table.of[WithList]("t").columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val vt = Table.of[WithVector]("t").columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    val ct = Table.of[WithChain]("t").columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]

    assertEquals(lt.find(_.name == "xs").get.tpe, Type._int4)
    assertEquals(vt.find(_.name == "xs").get.tpe, Type._int4)
    assertEquals(ct.find(_.name == "xs").get.tpe, Type._int4)
  }
}
