package skunk.sharp.contrib

import skunk.sharp.contrib.pgvector.*
import skunk.sharp.dsl.*

import scala.compiletime.testing.*

object PgVectorSuite {
  case class Chunk(id: Long, doc: String, content: String, embedding: PgVector[3])
  val chunks = Table.of[Chunk]("chunks").withPrimary("id")
}

class PgVectorSuite extends munit.FunSuite {
  import PgVectorSuite.*

  private val v = PgVector[3](0.1f, 0.2f, 0.3f)

  test("PgVector[N] checks its dimension at construction") {
    assertEquals(v.dimension, 3)
    val e = intercept[IllegalArgumentException](PgVector[3](1f, 2f))
    assert(e.getMessage.contains("dimension 3, got 2"), e.getMessage)
  }

  test("codec round-trips pgvector's text form and rejects the wrong dimension on decode") {
    val c = PgVector.codec[3]
    assertEquals(c.encode(v).flatten.map(_.value), List("[0.1,0.2,0.3]"))
    assertEquals(c.decode(0, List(Some("[1,2,3]"))).map(_.toArray.toList), Right(List(1f, 2f, 3f)))
    assert(c.decode(0, List(Some("[1,2]"))).isLeft)
  }

  test("Table.of derives vector(3) and records the extension for the validator") {
    val col = chunks.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]].find(_.name == "embedding").get
    assertEquals(col.tpe.name, "vector(3)")
    assertEquals(col.requiredExtension, Some("vector"))
  }

  test("distance operators render pgvector's symbols") {
    val q = chunks
      .select(c =>
        (
          c.embedding.cosineDistance(Param[PgVector[3]]),
          c.embedding.l2Distance(c.embedding),
          c.embedding.negativeInnerProduct(c.embedding),
          c.embedding.l1Distance(c.embedding)
        )
      )
      .compile
    assertEquals(
      q.fragment.sql,
      """SELECT "embedding" <=> $1, "embedding" <-> "embedding", "embedding" <#> "embedding", "embedding" <+> "embedding" FROM "chunks""""
    )
  }

  test("nearest-neighbour query: ORDER BY distance to a named query vector, LIMIT k") {
    val topK = chunks
      .select(c => (c.id, c.content))
      .where(c => c.doc === Param.named["doc", String])
      .orderBy(c => c.embedding.cosineDistance(Param.named["query", PgVector[3]]).asc)
      .limit(5)
      .compile
    assertEquals(
      topK.fragment.sql,
      """SELECT "id", "content" FROM "chunks" WHERE "doc" = $1 ORDER BY "embedding" <=> $2 ASC LIMIT 5"""
    )
    val af = topK.bind((doc = "handbook", query = v))
    assertEquals(af.fragment.encoder.encode(af.argument).flatten.map(_.value), List("handbook", "[0.1,0.2,0.3]"))
  }

  test("functions: vector_dims, vector_norm, l2_normalize, inner_product, avg") {
    val q = chunks
      .select(c =>
        (
          PgVector.dims(c.embedding),
          PgVector.norm(c.embedding),
          PgVector.normalize(c.embedding),
          PgVector.innerProduct(c.embedding, c.embedding)
        )
      )
      .compile
    assertEquals(
      q.fragment.sql,
      """SELECT vector_dims("embedding"), vector_norm("embedding"), l2_normalize("embedding"), inner_product("embedding", "embedding") FROM "chunks""""
    )
    val centroid = chunks.select(c => PgVector.avg(c.embedding)).compile
    assertEquals(centroid.fragment.sql, """SELECT avg("embedding") FROM "chunks"""")
  }

  test("comparing vectors of different dimensions does not compile") {
    val errs = typeCheckErrors("""
      import skunk.sharp.contrib.pgvector.*
      import skunk.sharp.dsl.*
      import PgVectorSuite.chunks
      chunks.select(c => c.embedding.cosineDistance(Param[PgVector[4]]))
    """)
    assert(errs.nonEmpty)
  }

  test("a deferred Param in a whole-row orderBy is a compile error (it would otherwise type as Void)") {
    val msg = typeCheckErrors("""
      import skunk.sharp.contrib.pgvector.*
      import skunk.sharp.dsl.*
      import PgVectorSuite.chunks
      chunks.select.orderBy(c => c.embedding.cosineDistance(Param[PgVector[3]]).asc)
    """).map(_.message).mkString("\n")
    assert(msg.contains("deferred Param in a whole-row .orderBy isn't supported"), msg)
  }
}
