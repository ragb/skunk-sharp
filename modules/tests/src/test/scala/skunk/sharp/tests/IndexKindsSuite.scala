package skunk.sharp.tests

import cats.effect.IO
import skunk.data.Arr
import skunk.sharp.{IndexDef, IndexKey}
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.pg.tags.PgRange
import skunk.sharp.validation.Mismatch

import java.time.{LocalDate, OffsetDateTime}

object IndexKindsSuite {

  case class Catalog(
    id: Long,
    sku: String,
    name: String,
    price: BigDecimal,
    tags: Arr[Int],
    available: PgRange[LocalDate],
    created_at: OffsetDateTime
  )

}

/**
 * Index methods, operator classes, collations, expressions, INCLUDE / WITH, standalone UNIQUE (V17__index_kinds.sql).
 */
class IndexKindsSuite extends PgFixture {
  import IndexKindsSuite.*

  private val base = Table.of[Catalog]("catalog").withPrimary("id").withDefault("created_at")

  private val declared = base
    .withIndexDef(IndexDef("catalog_tags_gin", IndexKey.column("tags")).withMethod("gin"))
    .withIndexDef(IndexDef("catalog_available_gist", IndexKey.column("available")).withMethod("gist"))
    .withIndexDef(IndexDef("catalog_created_brin", IndexKey.column("created_at")).withMethod("brin"))
    .withIndexDef(IndexDef("catalog_name_pattern_idx", IndexKey.column("name").opclass("text_pattern_ops")))
    .withIndexDef(IndexDef("catalog_name_c_idx", IndexKey.column("name").collate("C").desc))
    .withIndexDef(IndexDef("catalog_lower_name_idx", IndexKey.expr("lower(name)")))
    .withIndexDef(
      IndexDef("catalog_price_cover_idx", IndexKey.column("price")).include("name").withStorage("fillfactor" -> "70")
    )
    .withIndexDef(IndexDef("catalog_sku_uidx", IndexKey.column("sku")).unique)

  private def report(t: Table[?, ?]) =
    withContainers(containers => session(containers).use(s => SchemaValidator.validate[IO](s, t)))

  test("every index kind declared as in the migration validates clean") {
    report(declared).map(r => assert(r.isValid, r.mismatches.map(_.pretty).mkString("\n")))
  }

  test("an undeclared standalone UNIQUE index is reported (it isn't a constraint)") {
    val noUnique = base.withIndexDef(IndexDef("catalog_tags_gin", IndexKey.column("tags")).withMethod("gin"))
    report(noUnique).map { r =>
      val extras = r.mismatches.collect { case Mismatch.ExtraIndex(_, name, _) => name }.toSet
      assert(extras.contains("catalog_sku_uidx"), extras)
      assert(!extras.contains("catalog_pkey"), extras)
    }
  }

  test("a wrong method, operator class, collation, INCLUDE or storage parameter is a definition mismatch") {
    val wrong = declared
      .copy(indexes = Nil)
      .withIndexDef(IndexDef("catalog_tags_gin", IndexKey.column("tags")))                        // btree, DB has gin
      .withIndexDef(IndexDef("catalog_name_pattern_idx", IndexKey.column("name")))                // missing opclass
      .withIndexDef(IndexDef("catalog_name_c_idx", IndexKey.column("name").desc))                 // missing collation
      .withIndexDef(IndexDef("catalog_price_cover_idx", IndexKey.column("price")).include("sku")) // wrong INCLUDE
      .withIndexDef(IndexDef("catalog_sku_uidx", IndexKey.column("sku")))                         // not unique
    report(wrong).map { r =>
      val mismatched = r.mismatches.collect { case Mismatch.IndexDefinitionMismatch(_, n, _, _) => n }.toSet
      assertEquals(
        mismatched,
        Set(
          "catalog_tags_gin",
          "catalog_name_pattern_idx",
          "catalog_name_c_idx",
          "catalog_price_cover_idx",
          "catalog_sku_uidx"
        )
      )
    }
  }

  test("an unknown column in a declared index fails when the table is built") {
    intercept[IllegalArgumentException](base.withIndexDef(IndexDef("x", IndexKey.column("nope"))))
    intercept[IllegalArgumentException](base.withIndexDef(IndexDef("x", IndexKey.column("sku")).include("nope")))
  }
}
