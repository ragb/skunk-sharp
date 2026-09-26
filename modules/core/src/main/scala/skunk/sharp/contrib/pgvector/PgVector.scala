package skunk.sharp.contrib.pgvector

import cats.syntax.either.*
import skunk.Codec
import skunk.data.{Arr, Type}
import skunk.sharp.{PgFunction, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where

/**
 * pgvector's `vector(N)` — a fixed-dimension embedding (`CREATE EXTENSION vector;`). The dimension is part of the type,
 * so a query embedding from a different model (a different `N`) doesn't compile against the column, and a value of the
 * wrong length can't be constructed.
 *
 * {{{
 *   case class Chunk(id: Long, content: String, embedding: PgVector[1536])
 *   PgVector((0.1f, 0.2f, 0.3f))            // PgVector[3] — a literal can't have the wrong length
 *   PgVector.from[1536](modelOutput)        // Either[String, PgVector[1536]]
 * }}}
 *
 * Wire format is pgvector's text form `[x1,x2,…]`. Distances / functions: [[skunk.sharp.contrib.pgvector.ops]] and the
 * functions on this companion.
 *
 * A final class rather than an opaque alias: the DSL's Args match types must be able to prove the value type disjoint
 * from `Void` / tuples, which an opaque alias over `IArray` doesn't allow.
 */
final class PgVector[N <: Int] @scala.annotation.publicInBinary private (val values: IArray[Float]) {

  def dimension: Int        = values.length
  def toArray: Array[Float] = IArray.genericWrapArray(values).toArray

  override def equals(other: Any): Boolean = other match {
    case o: PgVector[?] => values.sameElements(o.values)
    case _              => false
  }

  override def hashCode: Int    = java.util.Arrays.hashCode(toArray)
  override def toString: String = values.mkString("PgVector[", ",", "]")
}

object PgVector {

  val RequiredExtension: String = "vector"

  /**
   * A literal vector whose dimension is fixed at compile time: `PgVector(0.1f, 0.2f, 0.3f)` is a `PgVector[3]` — the
   * values are counted when the code compiles, so assigning it to a `PgVector[4]` doesn't compile and nothing can
   * throw. For values that only exist at runtime (an embedding model's output), use [[from]].
   */
  transparent inline def apply(inline values: Float*): PgVector[? <: Int] = ${ PgVectorMacro.literal('values) }

  /** Used by the [[apply]] macro, which has already checked the count. */
  @scala.annotation.publicInBinary
  private[pgvector] def ofExactly[N <: Int](values: IArray[Float]): PgVector[N] = new PgVector[N](values)

  /** Exactly `N` values (e.g. an embedding model's output), or why not. */
  def from[N <: Int](values: IterableOnce[Float])(using n: ValueOf[N]): Either[String, PgVector[N]] = {
    val arr = IArray.from(values)
    if (arr.length == n.value) new PgVector[N](arr).asRight
    else s"expected a vector of dimension ${n.value}, got ${arr.length}".asLeft
  }

  /** Like [[from]], but throws `IllegalArgumentException` on the wrong length. */
  def unsafeFrom[N <: Int](values: IterableOnce[Float])(using ValueOf[N]): PgVector[N] =
    from[N](values).fold(e => throw new IllegalArgumentException(e), identity)

  private def render(v: PgVector[?]): String = v.values.mkString("[", ",", "]")

  private def parse[N <: Int](s: String)(using n: ValueOf[N]): Either[String, PgVector[N]] = {
    val body = s.trim.stripPrefix("[").stripSuffix("]")
    Either
      .catchNonFatal(if (body.isEmpty) IArray.empty[Float] else IArray.from(body.split(',').map(_.trim.toFloat)))
      .leftMap(e => s"invalid vector '$s': ${e.getMessage}")
      .flatMap(from[N](_))
  }

  /**
   * Codec on pgvector's text form. Its wire type is plain `vector` — Postgres reports result columns without the
   * dimension, so skunk's column-alignment check would reject `vector(N)` — while [[pgTypeFor]] declares `vector(N)`
   * for the schema validator.
   */
  def codec[N <: Int](using n: ValueOf[N]): Codec[PgVector[N]] =
    Codec.simple[PgVector[N]](v => render(v), s => parse[N](s), Type("vector"))

  given pgTypeFor[N <: Int](using n: ValueOf[N]): PgTypeFor[PgVector[N]] =
    new PgTypeFor[PgVector[N]] {
      val codec: Codec[PgVector[N]]                      = PgVector.codec[N]
      override val requiredExtension: Option[String]     = Some(RequiredExtension)
      override val declaredType: Option[skunk.data.Type] = Some(Type(s"vector(${n.value})"))
    }

  /** `vector[]` — lets a batch of embeddings travel as one array parameter (`Pg.unnestRows` / `unnestAsRelation`). */
  given arrPgTypeFor[N <: Int](using ValueOf[N]): PgTypeFor[Arr[PgVector[N]]] =
    PgTypeFor.instanceWithExtension(
      Codec.array[PgVector[N]](v => render(v), s => parse[N](s), Type("_vector", List(Type("vector")))),
      RequiredExtension
    )

  // ---- Functions -------------------------------------------------------------------------------

  /** `vector_dims(v)` — the dimension. */
  inline def dims[N <: Int, A](v: TypedExpr[PgVector[N], A]): TypedExpr[Int, A] =
    PgFunction.unary[PgVector[N], Int, A]("vector_dims")(v)

  /** `vector_norm(v)` — Euclidean norm. */
  inline def norm[N <: Int, A](v: TypedExpr[PgVector[N], A]): TypedExpr[Double, A] =
    PgFunction.unary[PgVector[N], Double, A]("vector_norm")(v)

  /** `l2_normalize(v)` — scale to unit length (pgvector 0.7+). */
  inline def normalize[N <: Int, A](v: TypedExpr[PgVector[N], A])(using ValueOf[N]): TypedExpr[PgVector[N], A] =
    PgFunction.unary[PgVector[N], PgVector[N], A]("l2_normalize")(v)

  /** `inner_product(a, b)` — the (positive) inner product; the `<#>` operator returns its negation. */
  inline def innerProduct[N <: Int, A, B](
    a: TypedExpr[PgVector[N], A],
    b: TypedExpr[PgVector[N], B]
  ): TypedExpr[Double, Where.Concat[A, B]] =
    PgFunction.binary[PgVector[N], PgVector[N], Double, A, B]("inner_product")(a, b)

  /** `avg(v)` — element-wise mean of the vectors in the group (a centroid). */
  inline def avg[N <: Int, A](v: TypedExpr[PgVector[N], A])(using ValueOf[N]): TypedExpr[PgVector[N], A] =
    PgFunction.unary[PgVector[N], PgVector[N], A]("avg")(v)

}
