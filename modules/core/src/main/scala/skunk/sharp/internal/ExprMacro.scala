package skunk.sharp.internal

import skunk.{Encoder, Fragment, Void}
import skunk.sharp.{RawExprBuilder, TypedExpr}
import skunk.sharp.where.Where
import skunk.util.Origin

import scala.quoted.*

/**
 * Macro behind the `expr"..."` interpolator (see [[skunk.sharp.expr]]).
 *
 * The job is exactly: concatenate the literal SQL pieces from the `StringContext` with each interpolated
 * [[TypedExpr]]'s `fragment.parts`, threading their `Args` slots through `Where.FoldConcat`. No value-baking, no
 * parameter wrapping — every interpolation must already be a `TypedExpr` (column ref, `Param[T]`, `lit(v)`, …). Literal
 * SQL pieces show up as `Left(s)` entries in the assembled `Fragment.parts`; arg fragments contribute their own parts
 * (which may carry `Right(state)` placeholders for `Param[T]`).
 */
private[sharp] object ExprMacro {

  def impl(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Expr[RawExprBuilder[?]] = {
    import quotes.reflect.*

    // Decode the StringContext literal parts (compile-time strings).
    val literalParts: List[String] = sc match {
      case '{ StringContext(${ Varargs(Exprs(parts)) }*) } => parts.toList
      case _                                               =>
        report.errorAndAbort(
          "skunk-sharp: `expr` interpolator literal parts must be compile-time strings.",
          sc
        )
    }

    val argExprs: List[Expr[Any]] = args match {
      case Varargs(es) => es.toList
      case _           =>
        report.errorAndAbort("skunk-sharp: `expr` interpolator could not decode interpolated args.", args)
    }

    if (literalParts.size != argExprs.size + 1)
      report.errorAndAbort(
        s"skunk-sharp: `expr` interpolator parts/args arity mismatch " +
          s"(parts=${literalParts.size}, args=${argExprs.size})."
      )

    // Per arg: (its Args TypeRepr, the Expr[Fragment[?]] to splice). `TypedExpr` is invariant, so we check
    // class hierarchy via `baseClasses`. We also `simplified` the term type to reduce match-type aliases
    // (e.g. `NamedTuple.Elem[…, 1]` for a column field access on a `ColumnsView` lambda parameter), then
    // coerce via a `Typed` wrapper so the splice's `.fragment` projection sees the concrete `TypedExpr` type.
    val typedExprSym                                  = TypeRepr.of[TypedExpr[Any, Any]].typeSymbol
    val argSlots: List[(TypeRepr, Expr[Fragment[?]])] = argExprs.map { argExpr =>
      val widened = argExpr.asTerm.tpe.widen.dealias.simplified

      if (widened.baseClasses.contains(typedExprSym)) {
        widened.baseType(typedExprSym) match {
          case AppliedType(_, List(tArgT, aTpe)) =>
            tArgT.asType match {
              case '[t] =>
                aTpe.asType match {
                  case '[a] =>
                    val coercedTerm = Typed(argExpr.asTerm, TypeTree.of[TypedExpr[t, a]])
                    val coerced     = coercedTerm.asExprOf[TypedExpr[t, a]]
                    val frag        = '{ $coerced.fragment }
                    (aTpe, frag.asExprOf[Fragment[?]])
                }
            }
          case other =>
            report.errorAndAbort(
              s"skunk-sharp: `expr` interpolator could not extract TypedExpr's Args parameter from " +
                s"${widened.show} (got ${other.show}).",
              argExpr
            )
        }
      } else {
        // Static-by-default: every interpolation must be a `TypedExpr`. Raw runtime values are rejected —
        // callers wrap explicitly with `lit(v)` / `Param[T]` / `Param.bind(v)`, mirroring every other DSL
        // operator position.
        report.errorAndAbort(
          s"skunk-sharp: `expr` interpolator argument has type ${widened.show}. Wrap it explicitly: " +
            s"`lit(v)` for a compile-time literal, `Param[T]` for an execute-time parameter, or " +
            s"`Param.bind(v)` to bake a runtime value into a Void-args fragment now.",
          argExpr
        )
      }
    }

    // Build the per-arg `Args` tuple `(A1, A2, …, An)` — no `Void` padding for literal SQL pieces; literals
    // are plain `Left(s)` strings in the final `Fragment.parts` and don't contribute to the encoder.
    val starCtr: TypeRepr = TypeRepr.of[Int *: EmptyTuple] match {
      case AppliedType(tc, _) => tc
      case other              =>
        report.errorAndAbort(s"skunk-sharp: could not recover *: type constructor from ${other.show}")
    }

    val tupTpe: TypeRepr = {
      val emptyT = TypeRepr.of[EmptyTuple]
      argSlots.map(_._1).foldRight(emptyT)((h, t) => starCtr.appliedTo(List(h, t)))
    }

    // Pre-compute the visible `Args` type at macro time so the `transparent inline` return type is fully
    // resolved at the call site (otherwise the abstract `t` would leak as `Nothing`). NOTE: we do *not*
    // call `.simplified` here — when `tupTpe` contains abstract type variables (e.g. inside an outer
    // generic helper body), simplifying `FoldConcat[A *: EmptyTuple]` re-enters match-type reduction with
    // unreducible scrutinees and the compiler can spin. The unreduced application is already correct; the
    // call-site expansion will reduce it once `A` is concrete.
    val argsTpe: TypeRepr =
      TypeRepr.of[Where.FoldConcat].appliedTo(tupTpe)

    val argFragsExpr: Expr[List[Fragment[?]]] = Expr.ofList(argSlots.map(_._2))
    val literalsExpr: Expr[List[String]]      = Expr.ofList(literalParts.map(Expr(_)))

    tupTpe.asType match {
      case '[tup] =>
        argsTpe.asType match {
          case '[args] =>
            '{
              ExprMacroRuntime.assemble[tup & Tuple, args]($literalsExpr, $argFragsExpr) match {
                case frag => new RawExprBuilder[args](frag)
              }
            }
        }
    }
  }

}

/**
 * Runtime helpers used by `expr"..."`-emitted code. Lives in a regular object so the assembly + encoder construction
 * don't get duplicated at every inline call site.
 */
object ExprMacroRuntime {

  /**
   * Direct concatenation: `literals(0)`, then for each `i`, `argFrags(i).parts ++ literals(i+1)`. The resulting
   * `Fragment.parts` is exactly the user's SQL with each interpolated `TypedExpr`'s parts spliced in — including any
   * `$N` placeholders the args carry. The encoder folds the args' encoders into one that consumes a
   * `Where.FoldConcat[Tup]` value and emits each arg's encoded bytes in render order.
   */
  inline def assemble[Tup <: Tuple, Args](
    literals: List[String],
    argFrags: List[Fragment[?]]
  ): Fragment[Args] = {
    val parts: List[Either[String, cats.data.State[Int, String]]] = {
      val buf  = scala.collection.mutable.ListBuffer.empty[Either[String, cats.data.State[Int, String]]]
      val head = literals.head
      if (head.nonEmpty) buf += Left(head)
      argFrags.zip(literals.tail).foreach { case (f, lit) =>
        buf ++= f.parts
        if (lit.nonEmpty) buf += Left(lit)
      }
      buf.toList
    }
    val projector: Args => List[Any] = c =>
      Where.projectFoldConcat[Tup](c.asInstanceOf[Where.FoldConcat[Tup]])
    val enc = buildEncoder[Args](argFrags, projector)
    Fragment(parts, enc, Origin.unknown)
  }

  /**
   * Build an encoder that consumes the user-facing `Args` value (a flat tuple, scalar, or `Void`), walks `argFrags` in
   * render order, and for each non-`Void` slot delegates to its arg's encoder. The projector is materialised at the
   * inline call site so `Where.projectFoldConcat[Tup]` reduces with the concrete tuple shape; this method itself is a
   * non-inline def so the anonymous `Encoder` class isn't duplicated at every interpolation site.
   */
  private[sharp] def buildEncoder[Args](
    argFrags: List[Fragment[?]],
    projector: Args => List[Any]
  ): Encoder[Args] = new Encoder[Args] {
    override val types: List[skunk.data.Type] = argFrags.flatMap(_.encoder.types)

    override val sql: cats.data.State[Int, String] =
      cats.data.State { (n0: Int) =>
        argFrags.foldLeft((n0, "")) { case ((n, acc), f) =>
          val (n1, s) = f.encoder.sql.run(n).value
          (n1, acc + s)
        }
      }

    override def encode(combined: Args): List[Option[skunk.data.Encoded]] = {
      val values = projector(combined)
      if (values.size != argFrags.size)
        throw new IllegalStateException(
          s"skunk-sharp: `expr` interpolator encoder arity mismatch — " +
            s"got ${values.size} projected values, expected ${argFrags.size}."
        )
      argFrags.zip(values).flatMap { case (f, v) =>
        val e = f.encoder.asInstanceOf[Encoder[Any]]
        if (e eq Void.codec) Nil
        else e.encode(v)
      }
    }
  }

}
