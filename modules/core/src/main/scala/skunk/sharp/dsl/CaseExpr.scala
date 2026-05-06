package skunk.sharp.dsl

import skunk.{Codec, Encoder, Fragment, Void}
import skunk.sharp.TypedExpr

/**
 * `CASE WHEN … THEN … [ELSE …] END` — the universal conditional primitive. Usable in SELECT projections,
 * WHERE, ORDER BY, UPDATE SET right-hand sides, GROUP BY — anywhere a [[TypedExpr]] is accepted.
 *
 * Branch predicates and bodies may carry their own `Args` (e.g. `Param[T]`). `CaseWhen` carries `Items <:
 * Tuple` accumulating each `(cond, branch)` pair as `TypedExpr[Boolean, A1] *: TypedExpr[T, A2] *: …`;
 * `.otherwise` / `.end` summon [[ProjArgsOf]] over the full item list (with ELSE appended for `.otherwise`)
 * to derive the result's typed `Args`. Branches with all `Args = Void` collapse cleanly to `Void`.
 *
 * Terminate with `.otherwise(elseBranch)` (returns `TypedExpr[T, OutA]`) or `.end` (returns
 * `TypedExpr[Option[T], OutA]`).
 */
final class CaseWhen[T, Items <: Tuple] private[sharp] (
  private[sharp] val branches:    List[(TypedExpr[Boolean, ?], TypedExpr[T, ?])],
  private[sharp] val branchCodec: Codec[T]
) {

  /** Append another `WHEN` branch. */
  def when[A1, A2](cond: TypedExpr[Boolean, A1], branch: TypedExpr[T, A2])
    : CaseWhen[T, Tuple.Concat[Items, TypedExpr[Boolean, A1] *: TypedExpr[T, A2] *: EmptyTuple]] =
    new CaseWhen(branches :+ (cond, branch), branchCodec)

  /** `ELSE <branch> END` — all paths covered, result is always a value of `T`. */
  def otherwise[ElseA, OutA](elseBranch: TypedExpr[T, ElseA])(using
    pa: ProjArgsOf.Aux[Tuple.Concat[Items, TypedExpr[T, ElseA] *: EmptyTuple], OutA]
  ): TypedExpr[T, OutA] =
    renderCaseWhen[T, OutA](branches, Some(elseBranch), branchCodec, pa.project.asInstanceOf[Any => List[Any]])

  /** `END` without an ELSE — Postgres returns NULL when no branch matches, so result is `Option[T]`. */
  def end[OutA](using pa: ProjArgsOf.Aux[Items, OutA]): TypedExpr[Option[T], OutA] = {
    val coercedBranches = branches.map { case (c, b) => (c, b.asInstanceOf[TypedExpr[Option[T], ?]]) }
    renderCaseWhen[Option[T], OutA](
      coercedBranches, None, branchCodec.opt, pa.project.asInstanceOf[Any => List[Any]]
    )
  }

}

/**
 * Build the CASE expression's parts list and a custom encoder that walks the typed slots in render
 * order — `[cond1, branch1, cond2, branch2, ..., (elseBranch?)]` — dispatching the user-supplied `OutA`
 * value through the [[ProjArgsOf]]-derived projector.
 */
private def renderCaseWhen[R, OutA](
  branches:  List[(TypedExpr[Boolean, ?], TypedExpr[R, ?])],
  elseOpt:   Option[TypedExpr[R, ?]],
  codec0:    Codec[R],
  projector: Any => List[Any]
): TypedExpr[R, OutA] = {
  val typedItems: List[Fragment[?]] =
    branches.flatMap { case (c, b) => List(c.fragment, b.fragment) } ++
      elseOpt.toList.map(_.fragment)

  val partsBuf = scala.collection.mutable.ListBuffer[Either[String, cats.data.State[Int, String]]]()
  partsBuf += Left("CASE")
  branches.foreach { case (c, b) =>
    partsBuf += Left(" WHEN ")
    partsBuf ++= c.fragment.parts
    partsBuf += Left(" THEN ")
    partsBuf ++= b.fragment.parts
  }
  elseOpt.foreach { e =>
    partsBuf += Left(" ELSE ")
    partsBuf ++= e.fragment.parts
  }
  partsBuf += Left(" END")

  val enc: Encoder[OutA] = new Encoder[OutA] {
    override val types: List[skunk.data.Type] = typedItems.flatMap(_.encoder.types)

    override val sql: cats.data.State[Int, String] =
      cats.data.State { (n0: Int) =>
        val sb = new StringBuilder("CASE")
        var n  = n0
        branches.foreach { case (c, b) =>
          sb ++= " WHEN "
          val (n1, cs) = c.fragment.encoder.sql.run(n).value
          sb ++= cs
          sb ++= " THEN "
          val (n2, bs) = b.fragment.encoder.sql.run(n1).value
          sb ++= bs
          n = n2
        }
        elseOpt.foreach { e =>
          sb ++= " ELSE "
          val (n3, es) = e.fragment.encoder.sql.run(n).value
          sb ++= es
          n = n3
        }
        sb ++= " END"
        (n, sb.result())
      }

    override def encode(args: OutA): List[Option[skunk.data.Encoded]] = {
      val values = projector(args)
      if (values.size != typedItems.size)
        throw new IllegalStateException(
          s"skunk-sharp: CASE projector arity mismatch — got ${values.size}, expected ${typedItems.size}"
        )
      typedItems.zip(values).flatMap { case (f, v) =>
        val e = f.encoder.asInstanceOf[Encoder[Any]]
        if (e eq Void.codec) Nil
        else e.encode(v)
      }
    }
  }

  val frag: Fragment[OutA] = Fragment(partsBuf.toList, enc, skunk.util.Origin.unknown)
  TypedExpr[R, OutA](frag, codec0)
}
