package skunk.sharp.internal

import skunk.{Encoder, Fragment}
import skunk.sharp.{TypedColumn, TypedExpr}
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.util.Origin

import scala.quoted.{Expr, Quotes, Type}

/**
 * Compile-time SQL assembly primitives. Each helper here is the macro-backed equivalent of a runtime
 * `PgOperator.infix(...)` call: when the LHS is statically known to be a `TypedColumn[_, _, N]` with a singleton
 * name `N`, the macro inlines the name and emits a single `skunk.Fragment` whose `parts` list has compile-time
 * constant strings, bypassing the runtime `|+|` chain.
 *
 * For LHS shapes we can't resolve at compile time (any `TypedExpr[T]` that isn't a `TypedColumn` literal — e.g.
 * `lower(col)`, `col.cast[_]`, a subquery `.asExpr`) the macro still produces a `Where[A]` whose visible `Args`
 * type is the RHS's scalar type — the LHS's existential bound parameters are baked in via `Encoder.contramap`.
 * Behaviour is identical either way.
 */
private[sharp] object SqlMacros {

  /**
   * Inline a binary infix comparison `lhs <op> <rhs>` where the RHS is a runtime value encoded as a parameter.
   * Both branches return `Where[S]` — the visible `Args` is concretely the RHS type.
   */
  inline def infix[T, S](
    inline op:  String,
    inline lhs: TypedExpr[T],
    rhs:        S
  )(using pf: PgTypeFor[S]): Where[S] =
    ${ infixImpl[T, S]('op, 'lhs, 'rhs, 'pf) }

  private def infixImpl[T: Type, S: Type](
    op:  Expr[String],
    lhs: Expr[TypedExpr[T]],
    rhs: Expr[S],
    pf:  Expr[PgTypeFor[S]]
  )(using q: Quotes): Expr[Where[S]] = {
    import q.reflect.*

    val opStr: String         = op.valueOrAbort
    val opSuffix: Expr[String] = Expr(s" $opStr ")

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col       = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          val enc       = $pf.codec
          val bakedFrag = skunk.Fragment(
            List(Left(col.sqlRef + $opSuffix), Right(enc.sql)),
            enc,
            skunk.util.Origin.unknown
          )
          Where[S](bakedFrag, $rhs)
        }
      case _ =>
        '{
          // Non-TypedColumn LHS: render the LHS to AppliedFragment, then bake its args via contramap so the
          // resulting Where carries Args = S (just the RHS) while still holding the LHS's bound values inside
          // the encoder.
          val lhsAf  = $lhs.render
          val rhsEnc = $pf.codec
          SqlMacros.infixContramap[S](lhsAf, $opSuffix, rhsEnc, $rhs)
        }
    }
  }

  /**
   * Runtime helper for the non-`TypedColumn` LHS branch: takes a pre-applied LHS fragment, an op-suffix string,
   * an RHS encoder + value, and produces a `Where[S]` whose fragment encodes only the RHS at the call site
   * while still rendering the LHS's bound values inline.
   */
  private[sharp] def infixContramap[S](
    lhsAf:    skunk.AppliedFragment,
    opSuffix: String,
    rhsEnc:   Encoder[S],
    rhs:      S
  ): Where[S] = {
    val lhsEnc: Encoder[Any] = lhsAf.fragment.encoder.asInstanceOf[Encoder[Any]]
    val lhsArg: Any          = lhsAf.argument
    val parts                = lhsAf.fragment.parts ++ List(Left(opSuffix), Right(rhsEnc.sql))
    val combinedEnc: Encoder[S] =
      lhsEnc.product(rhsEnc).contramap[S](r => (lhsArg, r))
    val frag: Fragment[S] = Fragment(parts, combinedEnc, Origin.unknown)
    Where[S](frag, rhs)
  }

  /**
   * Inline a 2-argument infix predicate: `<lhs> <sep1> <rhs1> <sep2> <rhs2>` — `BETWEEN lo AND hi`,
   * `NOT BETWEEN lo AND hi`, `BETWEEN SYMMETRIC lo AND hi`. Returns `Where[(S, S)]` — the two RHS values are
   * the visible Args tuple. Non-TypedColumn LHS uses the same contramap technique to preserve typed Args.
   */
  inline def infix2[T, S](
    inline sep1: String,
    inline sep2: String,
    inline lhs:  TypedExpr[T],
    rhs1:        S,
    rhs2:        S
  )(using pf: PgTypeFor[S]): Where[(S, S)] =
    ${ infix2Impl[T, S]('sep1, 'sep2, 'lhs, 'rhs1, 'rhs2, 'pf) }

  private def infix2Impl[T: Type, S: Type](
    sep1: Expr[String],
    sep2: Expr[String],
    lhs:  Expr[TypedExpr[T]],
    rhs1: Expr[S],
    rhs2: Expr[S],
    pf:   Expr[PgTypeFor[S]]
  )(using q: Quotes): Expr[Where[(S, S)]] = {
    import q.reflect.*

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          val enc = $pf.codec
          val parts: List[Either[String, cats.data.State[Int, String]]] = List(
            Left(col.sqlRef + " " + $sep1 + " "),
            Right(enc.sql),
            Left(" " + $sep2 + " "),
            Right(enc.sql)
          )
          val combinedEnc = enc.product(enc)
          val frag: skunk.Fragment[(S, S)] = skunk.Fragment(parts, combinedEnc, skunk.util.Origin.unknown)
          Where[(S, S)](frag, ($rhs1, $rhs2))
        }
      case _ =>
        '{
          SqlMacros.infix2Contramap[S]($lhs.render, $sep1, $sep2, $pf.codec, $rhs1, $rhs2)
        }
    }
  }

  /** Runtime helper for non-`TypedColumn` LHS in [[infix2]]. */
  private[sharp] def infix2Contramap[S](
    lhsAf: skunk.AppliedFragment,
    sep1:  String,
    sep2:  String,
    enc:   Encoder[S],
    rhs1:  S,
    rhs2:  S
  ): Where[(S, S)] = {
    val lhsEnc: Encoder[Any] = lhsAf.fragment.encoder.asInstanceOf[Encoder[Any]]
    val lhsArg: Any          = lhsAf.argument
    val parts =
      lhsAf.fragment.parts ++ List(
        Left(" " + sep1 + " "),
        Right(enc.sql),
        Left(" " + sep2 + " "),
        Right(enc.sql)
      )
    val rhsPair: Encoder[(S, S)] = enc.product(enc)
    val combined: Encoder[(S, S)] =
      lhsEnc.product(rhsPair).contramap[(S, S)](p => (lhsArg, p))
    val frag: Fragment[(S, S)] = Fragment(parts, combined, Origin.unknown)
    Where[(S, S)](frag, (rhs1, rhs2))
  }

  /**
   * Inline a postfix no-argument predicate `<lhs> <suffix>` — `IS NULL`, `IS NOT NULL`, `IS TRUE`, …. Returns
   * `Where[skunk.Void]` — no parameters captured.
   */
  inline def postfix[T](inline lhs: TypedExpr[T], inline suffix: String): Where[skunk.Void] =
    ${ postfixImpl[T]('lhs, 'suffix) }

  private def postfixImpl[T: Type](
    lhs:    Expr[TypedExpr[T]],
    suffix: Expr[String]
  )(using q: Quotes): Expr[Where[skunk.Void]] = {
    import q.reflect.*

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          val frag: skunk.Fragment[skunk.Void] =
            skunk.Fragment(
              List(Left(col.sqlRef + $suffix)),
              skunk.Void.codec,
              skunk.util.Origin.unknown
            )
          Where[skunk.Void](frag, skunk.Void)
        }
      case _ =>
        '{
          SqlMacros.postfixContramap($lhs.render, $suffix)
        }
    }
  }

  /** Runtime helper for non-`TypedColumn` LHS in [[postfix]]. */
  private[sharp] def postfixContramap(
    lhsAf:  skunk.AppliedFragment,
    suffix: String
  ): Where[skunk.Void] = {
    val lhsEnc: Encoder[Any] = lhsAf.fragment.encoder.asInstanceOf[Encoder[Any]]
    val lhsArg: Any          = lhsAf.argument
    val parts                = lhsAf.fragment.parts ++ List(Left(suffix))
    val voidEnc: Encoder[skunk.Void] =
      lhsEnc.contramap[skunk.Void](_ => lhsArg)
    val frag: Fragment[skunk.Void] = Fragment(parts, voidEnc, Origin.unknown)
    Where[skunk.Void](frag, skunk.Void)
  }

  /**
   * Build a parameterless `AppliedFragment` rendering the LHS followed by a literal op string. Used by operators
   * whose RHS is already assembled as an `AppliedFragment` at call time (e.g. `IN (…)` with a variadic value
   * list or a subquery). When the LHS is a `TypedColumn`, the whole `"<col> <op> "` literal is a single baked
   * `Fragment[Void]`; otherwise falls back to `lhs.render |+| raw(" <op> ")`.
   */
  inline def prefix[T](inline lhs: TypedExpr[T], inline op: String): skunk.AppliedFragment =
    ${ prefixImpl[T]('lhs, 'op) }

  private def prefixImpl[T: Type](
    lhs: Expr[TypedExpr[T]],
    op:  Expr[String]
  )(using q: Quotes): Expr[skunk.AppliedFragment] = {
    import q.reflect.*

    def runtime: Expr[skunk.AppliedFragment] =
      '{ $lhs.render |+| TypedExpr.raw(" " + $op + " ") }

    lhs.asTerm.tpe.widen.asType match {
      case '[TypedColumn[t, nb, nm]] =>
        '{
          val col = $lhs.asInstanceOf[TypedColumn[t, nb, nm]]
          skunk.AppliedFragment(
            skunk.Fragment(
              List(Left(col.sqlRef + " " + $op + " ")),
              skunk.Void.codec,
              skunk.util.Origin.unknown
            ),
            skunk.Void
          )
        }
      case _ => runtime
    }
  }

  /**
   * Legacy POC helper — kept for the explicit-baked-path test that exercises the macro without going through
   * the `===` / `>=` / ... extension methods.
   */
  inline def columnValOp[T, Null <: Boolean, N <: String & Singleton](
    inline col: TypedColumn[T, Null, N],
    inline op:  String,
    rhs:        T
  )(using pf: PgTypeFor[T]): Where[T] =
    infix[T, T](op, col, rhs)

}
