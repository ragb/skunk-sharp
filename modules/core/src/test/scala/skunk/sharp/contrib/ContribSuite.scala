package skunk.sharp.contrib

import skunk.sharp.{ColumnsView, Table}
import skunk.sharp.contrib.citext.{Citext, PgCitext}
import skunk.sharp.contrib.fuzzystrmatch.PgFuzzy
import skunk.sharp.contrib.hstore.{Hstore, PgHstore}
import skunk.sharp.contrib.hstore.*
import skunk.sharp.contrib.ltree.{LQuery, LTree, PgLtree}
import skunk.sharp.contrib.ltree.*
import skunk.sharp.contrib.pgcrypto.PgCrypto
import skunk.sharp.contrib.pgtrgm.PgTrgm
import skunk.sharp.contrib.pgtrgm.*
import skunk.sharp.dsl.*
import skunk.sharp.pg.PgTypeFor

object ContribSuite {

  case class CitextRow(id: Int, email: Citext)
  case class LtreeRow(id: Int, path: LTree)
  case class HstoreRow(id: Int, props: Hstore)
}

class ContribSuite extends munit.FunSuite {
  import ContribSuite.*

  // ---------- citext -----------------------------------------------------------------------------

  test("citext PgTypeFor carries requiredExtension = 'citext'") {
    assertEquals(PgTypeFor[Citext].requiredExtension, Some("citext"))
  }

  test("citext column propagates requiredExtension to relation") {
    val t = Table.of[CitextRow]("users")
    assertEquals(t.requiredExtensions, Set("citext"))
  }

  test("Option[Citext] preserves the requiredExtension on the codec") {
    assertEquals(PgTypeFor[Option[Citext]].requiredExtension, Some("citext"))
  }

  test("Citext flows through existing string operators (=== / like)") {
    val t    = Table.of[CitextRow]("users")
    val cols = ColumnsView(t.columns)
    val w    = cols.email === Param.bind(Citext("alice@example.com"))
    assertEquals(w.fragment.sql, """"email" = $1""")
  }

  test("PgCitext.toCitext renders ::citext cast") {
    val t    = Table.of[CitextRow]("users")
    val cols = ColumnsView(t.columns)
    val e    = PgCitext.toCitext(lit("Alice@Example.COM"))
    assertEquals(e.fragment.sql, "('Alice@Example.COM')::citext")
    val _ = cols
  }

  // ---------- ltree ------------------------------------------------------------------------------

  test("ltree PgTypeFor carries requiredExtension = 'ltree'") {
    assertEquals(PgTypeFor[LTree].requiredExtension, Some("ltree"))
    assertEquals(PgTypeFor[LQuery].requiredExtension, Some("ltree"))
  }

  test("ltree column propagates requiredExtension to relation") {
    val t = Table.of[LtreeRow]("tags")
    assertEquals(t.requiredExtensions, Set("ltree"))
  }

  test("ltree operators render correct SQL") {
    val t      = Table.of[LtreeRow]("tags")
    val cols   = ColumnsView(t.columns)
    val ances  = cols.path.isAncestorOf(Param.bind(LTree("top.science")))
    val descs  = cols.path.isDescendantOf(Param.bind(LTree("top.science")))
    val concat = cols.path.concat(Param.bind(LTree("astronomy")))
    val patt   = cols.path.matches(Param.bind(LQuery("top.*.astronomy")))
    assertEquals(ances.fragment.sql, """"path" @> $1""")
    assertEquals(descs.fragment.sql, """"path" <@ $1""")
    assertEquals(concat.fragment.sql, """"path" || $1""")
    assertEquals(patt.fragment.sql, """"path" ~ $1""")
  }

  test("PgLtree.nlevel / lca render") {
    val t    = Table.of[LtreeRow]("tags")
    val cols = ColumnsView(t.columns)
    assertEquals(PgLtree.nlevel(cols.path).fragment.sql, """nlevel("path")""")
    assertEquals(
      PgLtree.lca(cols.path, Param.bind(LTree("top.science"))).fragment.sql,
      """lca("path", $1)"""
    )
  }

  // ---------- pg_trgm ----------------------------------------------------------------------------

  test("pg_trgm RequiredExtension is wired") {
    assertEquals(PgTrgm.RequiredExtension, "pg_trgm")
  }

  test("trigram operators render correct SQL") {
    val t    = Table.of[(Int, String)]("dummy")
    // Bypass Table.of[Tuple]; use builder explicitly for a string col
    val users = Table.builder("docs").column[String]("body").build
    val cols  = ColumnsView(users.columns)
    val sim   = cols.body.similarTrgm(lit("widget"))
    val dist  = cols.body.trgmDistance(lit("widget"))
    val w     = cols.body.wordSimilar(lit("wid"))
    assertEquals(sim.fragment.sql, """"body" % 'widget'""")
    assertEquals(dist.fragment.sql, """"body" <-> 'widget'""")
    assertEquals(w.fragment.sql, """"body" <% 'wid'""")
    val _ = t
  }

  test("PgTrgm.similarity / wordSimilarity render") {
    val users = Table.builder("docs").column[String]("body").build
    val cols  = ColumnsView(users.columns)
    assertEquals(
      PgTrgm.similarity(cols.body, lit("widget")).fragment.sql,
      """similarity("body", 'widget')"""
    )
    assertEquals(
      PgTrgm.wordSimilarity(cols.body, lit("widget")).fragment.sql,
      """word_similarity("body", 'widget')"""
    )
  }

  // ---------- pgcrypto ---------------------------------------------------------------------------

  test("pgcrypto RequiredExtension is wired") {
    assertEquals(PgCrypto.RequiredExtension, "pgcrypto")
  }

  test("crypt / gen_salt / hmac render correct SQL") {
    assertEquals(
      PgCrypto.crypt(lit("hunter2"), lit("$2a$10$abc")).fragment.sql,
      """crypt('hunter2', '$2a$10$abc')"""
    )
    assertEquals(PgCrypto.genSalt(lit("bf")).fragment.sql, """gen_salt('bf')""")
    assertEquals(PgCrypto.genSalt(lit("bf"), lit(10)).fragment.sql, """gen_salt('bf', 10)""")
    assertEquals(
      PgCrypto.hmac(lit("data"), lit("key"), lit("sha256")).fragment.sql,
      """hmac('data', 'key', 'sha256')"""
    )
  }

  // ---------- fuzzystrmatch ----------------------------------------------------------------------

  test("fuzzystrmatch RequiredExtension is wired") {
    assertEquals(PgFuzzy.RequiredExtension, "fuzzystrmatch")
  }

  test("levenshtein / soundex / metaphone render correct SQL") {
    assertEquals(PgFuzzy.levenshtein(lit("foo"), lit("bar")).fragment.sql, """levenshtein('foo', 'bar')""")
    assertEquals(
      PgFuzzy.levenshtein(lit("foo"), lit("bar"), lit(1), lit(1), lit(2)).fragment.sql,
      """levenshtein('foo', 'bar', 1, 1, 2)"""
    )
    assertEquals(PgFuzzy.soundex(lit("Robert")).fragment.sql, """soundex('Robert')""")
    assertEquals(PgFuzzy.metaphone(lit("Robert"), lit(4)).fragment.sql, """metaphone('Robert', 4)""")
    assertEquals(PgFuzzy.dmetaphone(lit("Robert")).fragment.sql, """dmetaphone('Robert')""")
  }

  // ---------- hstore -----------------------------------------------------------------------------

  test("hstore PgTypeFor carries requiredExtension = 'hstore'") {
    assertEquals(PgTypeFor[Hstore].requiredExtension, Some("hstore"))
  }

  test("hstore column propagates requiredExtension to relation") {
    val t = Table.of[HstoreRow]("things")
    assertEquals(t.requiredExtensions, Set("hstore"))
  }

  test("hstore operators render correct SQL") {
    val t    = Table.of[HstoreRow]("things")
    val cols = ColumnsView(t.columns)
    assertEquals(cols.props.hasKey(lit("k")).fragment.sql, """"props" ? 'k'""")
    assertEquals(
      cols.props.contains(Param.bind(Hstore("k" -> Some("v")))).fragment.sql,
      """"props" @> $1"""
    )
    assertEquals(cols.props.deleteKey(lit("k")).fragment.sql, """"props" - 'k'""")
    val _ = PgHstore.hstoreToJson(cols.props)
    val _ = PgHstore.defined(cols.props, lit("k"))
  }

  test("hstore codec round-trips a mixed Some/None value") {
    val src    = Hstore("a" -> Some("1"), "b" -> None, "q\"x" -> Some("v\\y"))
    val codec  = Hstore.codec
    val enc    = codec.encode(src).headOption.flatten.getOrElse(fail("encoding failed"))
    val parsed = codec.decode(0, List(Some(enc.value))).fold(err => fail(err.message), identity)
    assertEquals(parsed, src)
  }

  // ---------- Multi-extension relation aggregation -----------------------------------------------

  test("a relation with two contrib columns reports both extensions") {
    case class Row(id: Int, e: Citext, p: LTree)
    val t = Table.of[Row]("multi")
    assertEquals(t.requiredExtensions, Set("citext", "ltree"))
  }

  // ---------- Auto-discovery via codec.tpe -------------------------------------------------------

  test("explicit-codec column path auto-discovers extension from codec.tpe") {
    val t = Table.builder("rooms")
      .column("id", skunk.codec.all.uuid)
      .column("location", LTree.codec)
      .column("amenities", Hstore.codec)
      .build
    assertEquals(t.requiredExtensions, Set("ltree", "hstore"))
  }

  test("withColumnCodec override re-uses the codec's tpe for extension discovery") {
    case class Row(id: Int, name: String)
    val t = Table.of[Row]("rooms").withColumnCodec("name", Citext.codec)
    assertEquals(t.requiredExtensions, Set("citext"))
  }

  test("View built with explicit codec also picks up extensions") {
    val v = View.builder("room_paths")
      .column("id", skunk.codec.all.int4)
      .column("path", LTree.codec)
      .build
    assertEquals(v.requiredExtensions, Set("ltree"))
  }

  test("lquery / ltxtquery types also map to the ltree extension") {
    val t = Table.builder("patterns")
      .column("id", skunk.codec.all.int4)
      .column("pattern", LQuery.codec)
      .build
    assertEquals(t.requiredExtensions, Set("ltree"))
  }
}
