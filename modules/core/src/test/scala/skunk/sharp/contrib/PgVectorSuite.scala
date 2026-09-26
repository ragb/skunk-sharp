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

  private val v = PgVector(0.1f, 0.2f, 0.3f)

  test("PgVector(…) with a spliced runtime collection, or no values, doesn't compile") {
    val spliced = typeCheckErrors("""
      import skunk.sharp.contrib.pgvector.*
      val xs = List(1f, 2f, 3f)
      PgVector(xs*)
    """).map(_.message).mkString("\n")
    assert(spliced.contains("PgVector.from[N]"), spliced)
    val empty = typeCheckErrors("""
      import skunk.sharp.contrib.pgvector.*
      PgVector()
    """).map(_.message).mkString("\n")
    assert(empty.contains("at least one value"), empty)
  }

  test("PgVector(…) counts its values at compile time — a dimension mismatch doesn't compile") {
    val three: PgVector[3] = v
    assertEquals(three.dimension, 3)
    val errs = typeCheckErrors("""
      import skunk.sharp.contrib.pgvector.*
      val four: PgVector[4] = PgVector(1f, 2f, 3f)
    """)
    assert(errs.nonEmpty)
  }

  test("from returns Either; unsafeFrom throws on the wrong length") {
    assertEquals(PgVector.from[3](List(1f, 2f, 3f)).map(_.toArray.toList), Right(List(1f, 2f, 3f)))
    assertEquals(PgVector.from[3](Array(1f, 2f)).left.map(_.contains("dimension 3, got 2")), Left(true))
    val e = intercept[IllegalArgumentException](PgVector.unsafeFrom[3](List(1f, 2f)))
    assert(e.getMessage.contains("dimension 3, got 2"), e.getMessage)
  }

  test("codec round-trips pgvector's text form and rejects the wrong dimension on decode") {
    val c = PgVector.codec[3]
    assertEquals(c.encode(v).flatten.map(_.value), List("[0.1,0.2,0.3]"))
    assertEquals(c.decode(0, List(Some("[1,2,3]"))).map(_.toArray.toList), Right(List(1f, 2f, 3f)))
    assert(c.decode(0, List(Some("[1,2]"))).isLeft)
  }

  test("Table.of declares vector(3) for the validator; the codec's wire type is plain vector") {
    val col = chunks.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]].find(_.name == "embedding").get
    assertEquals(col.tpe.name, "vector(3)")
    assertEquals(col.requiredExtension, Some("vector"))
    // Postgres reports result columns without the dimension; asserting vector(3) on decode would fail skunk's check.
    assertEquals(PgVector.codec[3].types.map(_.name), List("vector"))
  }

  test("TableBuilder.column[PgVector[N]] declares vector(N) too, nullable or not") {
    val t    = Table.builder("chunks").column[PgVector[3]]("embedding").columnOpt[PgVector[3]]("draft").build
    val cols = t.columns.toList.asInstanceOf[List[skunk.sharp.Column[?, ?, ?, ?]]]
    assertEquals(cols.map(_.tpe.name), List("vector(3)", "vector(3)"))
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

  test("whole-row top-k: a deferred query vector in orderBy is a typed Param") {
    val q = chunks.select.orderBy(c => c.embedding.cosineDistance(Param[PgVector[3]]).asc).limit(5).compile
    val _: QueryTemplate[PgVector[3], ?] = q
    assertEquals(
      q.fragment.sql.trim,
      """SELECT "id", "doc", "content", "embedding" FROM "chunks" ORDER BY "embedding" <=> $1 ASC LIMIT 5"""
    )
  }

  test("vector arithmetic through the core operators: + - and element-wise *") {
    val q = chunks.select(c =>
      (c.embedding + c.embedding, c.embedding - Param[PgVector[3]], c.embedding * c.embedding)
    ).compile
    val _: QueryTemplate[PgVector[3], (PgVector[3], PgVector[3], PgVector[3])] = q
    assertEquals(
      q.fragment.sql,
      """SELECT ("embedding" + "embedding"), ("embedding" - $1), ("embedding" * "embedding") FROM "chunks""""
    )
  }
}
