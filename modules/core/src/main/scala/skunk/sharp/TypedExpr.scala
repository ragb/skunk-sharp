package skunk.sharp

import skunk.{AppliedFragment, Codec, Encoder, Fragment, Void}
import skunk.sharp.internal.RawConstants
import skunk.sharp.pg.PgTypeFor
import skunk.sharp.where.Where
import skunk.util.Origin

/**
 * A typed SQL expression — the universal vocabulary of the DSL.
 *
 * `T` is the Scala value type the expression decodes to. `Args` is the tuple of values that must be supplied at
 * **execute time** for any `$N` parameter placeholders the expression carries. A column reference contributes no
 * params — its `Args` is `skunk.Void`. A `Param[Int]` contributes one — its `Args` is `Int`. A composed expression
 * (`Pg.lower(col1) ++ col2 || Param[String]`) accumulates the inputs' Args via the [[Where.Concat]] type-level pair
 * reduction (drops `Void` placeholders).
 *
 * Everything operators, functions, WHERE clauses, SELECT projections, INSERT VALUES, UPDATE SET RHS produce and
 * consume is a `TypedExpr[T, A]`. The primitive leaves are [[TypedColumn]] (a column reference,
 * `TypedExpr[T, Void]`), `TypedExpr.lit` (an inline primitive literal, `TypedExpr[T, Void]`), and [[Param]] (a
 * deferred parameter, `TypedExpr[T, T]`). Third-party modules add new operators and functions by returning
 * `TypedExpr[..., A]` whose Args reflect what their inputs contribute.
 */
trait TypedExpr[T, Args] {

  /** Typed Fragment carrying the SQL parts + the encoder that consumes `Args` at execute time. */
  def fragment: Fragment[Args]

  /** Codec for decoding the resulting Postgres value into `T`. */
  def codec:    Codec[T]
}

object TypedExpr {

  /**
   * Construct a `TypedExpr` whose `fragment` is computed **lazily** — by-name argument plus `lazy val` inside.
   * Operators, functions, and subquery lifters (`.asExpr`, `Pg.exists`, …) compose other `TypedExpr`s by touching
   * their `fragment`; chaining off this one means nothing is materialised until the outermost
   * `SelectBuilder.compile` / `ProjectedSelect.compile` walks the expression tree and pulls on `fragment`.
   *
   * The laziness matters most for correlated subquery uses (`Pg.exists(inner)`) where `inner` is a
   * `SelectBuilder` whose own `.compile` only works once the outer view has been fixed.
   */
  def apply[T, A](frag: => Fragment[A], codec0: Codec[T]): TypedExpr[T, A] = {
    val c = codec0
    new TypedExpr[T, A] {
      lazy val fragment: Fragment[A] = frag
      val codec: Codec[T]            = c
    }
  }

  /**
   * Lift a **compile-time primitive literal** into a `TypedExpr[T, Void]`. Supported literal types: `Boolean`,
   * `Int`, `Long`, `Short`, `Byte`, `Float`, `Double`. The literal is rendered inline in the SQL text
   * (`TRUE` / `42` / `'Infinity'::float8` / …), never as a bound `$N` parameter. Args is `Void` (no execute-time
   * input needed).
   *
   * Anything else — runtime variables, strings, UUIDs, timestamps, arrays, refined / tag types, user-defined — is
   * a **compile error**. The error message points at:
   *
   *   - WHERE / SELECT / SET operators with a [[Param]] arg for execute-time-bound values, OR
   *   - [[Param.bind]] for the rare "I want this runtime value baked into a Void-args fragment right now" path.
   */
  inline def lit[T](inline value: T)(using pf: PgTypeFor[T]): TypedExpr[T, Void] =
    ${ litMacro.impl[T]('value, 'pf) }

  /**
   * Build a `Fragment[Void]` whose parts are the supplied SQL with `Right(codec.sql)` placeholders, and whose
   * encoder bakes `value` via contramap. Returns `TypedExpr[T, Void]` — the value is fixed at construction time
   * and isn't supplied at execute. Used by the [[lit]] macro for runtime fallbacks and by [[Param.bind]] as the
   * "treat this runtime value as already-baked" escape hatch. **Most users want [[Param]] instead.**
   */
  def parameterised[T](value: T)(using pf: PgTypeFor[T]): TypedExpr[T, Void] = {
    val enc: Encoder[T]            = pf.codec
    val voidEnc: Encoder[Void]     = enc.contramap[Void](_ => value)
    val frag: Fragment[Void]       = Fragment(List(Right(enc.sql)), voidEnc, Origin.unknown)
    apply[T, Void](frag, pf.codec)
  }

  /** Render a `Float` literal as its SQL form. Always emits a `::float4` cast. */
  def renderFloat(v: Float): String = v match {
    case x if java.lang.Float.isNaN(x) => "'NaN'::float4"
    case Float.PositiveInfinity        => "'Infinity'::float4"
    case Float.NegativeInfinity        => "'-Infinity'::float4"
    case x                             => s"${x.toString}::float4"
  }

  /** Same as [[renderFloat]] for `Double` — emits a `::float8` cast for identical reasons. */
  def renderDouble(v: Double): String = v match {
    case x if java.lang.Double.isNaN(x) => "'NaN'::float8"
    case Double.PositiveInfinity        => "'Infinity'::float8"
    case Double.NegativeInfinity        => "'-Infinity'::float8"
    case x                              => s"${x.toString}::float8"
  }

  /**
   * Build an `AppliedFragment` from a SQL string — the structural-token escape hatch.
   *
   * Compile-time-constant arguments intern through a process-wide table at first call; runtime-built strings
   * always allocate fresh. **Used for SQL keywords / structural pieces** (parens, separators, `FROM`, `WHERE`)
   * that don't carry user-supplied parameters. For typed expression nodes, use [[apply]] / [[lit]] / [[Param]].
   */
  inline def raw(inline sql: String): AppliedFragment =
    ${ skunk.sharp.internal.RawMacro.impl('sql) }

  /**
   * Join a non-empty list of applied fragments with `sep` between them (e.g. " AND ").
   *
   * Fast path: all-static lists collapse to one interned AF. See structural use in `assemble` paths.
   */
  def joined(parts: List[AppliedFragment], sep: String): AppliedFragment =
    parts match {
      case Nil         => AppliedFragment.empty
      case head :: Nil => head
      case head :: tail if parts.forall(isStaticAf) =>
        val sb = new StringBuilder
        sb ++= staticString(head)
        tail.foreach { p =>
          sb ++= sep
          sb ++= staticString(p)
        }
        val str = sb.result()
        val frag: Fragment[Void] = Fragment(List(Left(str)), Void.codec, Origin.unknown)
        frag(Void)
      case head :: tail =>
        val sepAf = RawConstants.intern(sep)
        tail.foldLeft(head)((acc, p) => acc |+| sepAf |+| p)
    }

  private def isStaticAf(af: AppliedFragment): Boolean =
    af.fragment.parts.forall(_.isLeft)

  private def staticString(af: AppliedFragment): String = {
    val sb = new StringBuilder
    af.fragment.parts.foreach {
      case Left(s)  => sb ++= s
      case Right(_) => // unreachable when isStaticAf is true
    }
    sb.result()
  }

  // ---- Fragment composition helpers --------------------------------------------------------------

  /**
   * Pair two typed Fragments into a single one whose Args is `Where.Concat[A, B]`. The combined encoder
   * always products both sub-encoders (so any baked values riding on either side flow through), then
   * contramaps the user's `Concat[A, B]` input back into the `(A, B)` tuple the product consumes — see
   * [[Where.Concat2]]. The `eq Void.codec` shortcuts skip the product when one side is the literal Void
   * encoder (no params at all on that side).
   */
  private[sharp] def combine[A, B](
    a: Fragment[A], b: Fragment[B]
  )(using c2: Where.Concat2[A, B]): Fragment[Where.Concat[A, B]] = {
    val parts = a.parts ++ b.parts
    val enc   = combineEnc[A, B](a.encoder, b.encoder)
    Fragment(parts, enc, Origin.unknown)
  }

  /**
   * Pair two typed Fragments inserting a static separator between them. Equivalent to
   * `combine(a, combine(separatorFragment, b))` but allocates fewer intermediate fragments.
   */
  private[sharp] def combineSep[A, B](
    a: Fragment[A], sepSql: String, b: Fragment[B]
  )(using c2: Where.Concat2[A, B]): Fragment[Where.Concat[A, B]] = {
    val sepLeft: Either[String, cats.data.State[Int, String]] = Left(sepSql)
    val parts = a.parts ++ List(sepLeft) ++ b.parts
    val enc   = combineEnc[A, B](a.encoder, b.encoder)
    Fragment(parts, enc, Origin.unknown)
  }

  /**
   * Combine N typed `Fragment`s into one whose `Args` is the caller-claimed `Combined`. The encoder walks
   * `items` in render order: each non-`Void`-encoder item consumes one entry from `projector(args)`;
   * Void-encoder items emit nothing. `parts` are interleaved with `sep`.
   *
   * Used by variadic builders, RETURNING tuples, SELECT projections, GROUP BY / ORDER BY / DISTINCT ON
   * lists — anywhere N typed slots need to fold into one. The caller supplies a `projector` matching
   * `Combined` to the per-item values list; typically derived from a [[Where.FoldConcatN]] instance
   * whose `Combined = FoldConcat[CollectArgs[T]]`.
   */
  private[sharp] def combineList[Combined](
    items:     List[Fragment[?]],
    sep:       String,
    projector: Combined => List[Any]
  ): Fragment[Combined] = {
    if (items.isEmpty) voidFragment("").asInstanceOf[Fragment[Combined]]
    else {
      val sepLeft: Either[String, cats.data.State[Int, String]] = Left(sep)
      val parts: List[Either[String, cats.data.State[Int, String]]] =
        items.zipWithIndex.flatMap { case (f, i) =>
          if (i == 0) f.parts
          else sepLeft :: f.parts
        }
      val enc: Encoder[Combined] = new Encoder[Combined] {
        override val types: List[skunk.data.Type] = items.flatMap(_.encoder.types)

        override val sql: cats.data.State[Int, String] =
          cats.data.State { (n0: Int) =>
            items.zipWithIndex.foldLeft((n0, "")) { case ((n, acc), (f, i)) =>
              val (n1, s) = f.encoder.sql.run(n).value
              val sepStr  = if (i == 0) "" else sep
              (n1, acc + sepStr + s)
            }
          }

        override def encode(args: Combined): List[Option[skunk.data.Encoded]] = {
          val values = projector(args)
          if (values.size != items.size)
            throw new IllegalStateException(
              s"skunk-sharp: combineList projector arity mismatch — got ${values.size}, expected ${items.size}"
            )
          items.zip(values).flatMap { case (f, v) =>
            val e = f.encoder.asInstanceOf[Encoder[Any]]
            if (e eq Void.codec) Nil
            else e.encode(v)
          }
        }
      }
      Fragment(parts, enc, Origin.unknown)
    }
  }

  /**
   * Variadic Void-aware combine. Treats every input as `Fragment[Void]` (whether structurally Void or
   * Param.bind-baked Void), interleaves with `sep` between, returns `Fragment[Void]`. Used by variadic
   * function builders (`Pg.concat`, `Pg.coalesce`, `Pg.greatest`, `Pg.makeDate`, …) where mixing typed
   * `Param[T]` inputs is roadmap; for now everything must be Void-input on the inside.
   */
  private[sharp] def joinedVoid(sep: String, parts: List[Fragment[?]]): Fragment[Void] =
    parts match {
      case Nil          => voidFragment("")
      case head :: Nil  => head.asInstanceOf[Fragment[Void]]
      case head :: tail =>
        tail.foldLeft(head.asInstanceOf[Fragment[Void]]) { (acc, p) =>
          combineSep[Void, Void](acc, sep, p.asInstanceOf[Fragment[Void]])
        }
    }

  /** Pair two encoders, contramapping `Concat[A, B]` → `(A, B)` per [[Where.Concat2]]. */
  private[sharp] def combineEnc[A, B](
    a: Encoder[A], b: Encoder[B]
  )(using c2: Where.Concat2[A, B]): Encoder[Where.Concat[A, B]] = {
    val voidLeft  = a eq Void.codec
    val voidRight = b eq Void.codec
    if (voidLeft && voidRight) Void.codec.asInstanceOf[Encoder[Where.Concat[A, B]]]
    else if (voidLeft)         b.asInstanceOf[Encoder[Where.Concat[A, B]]]
    else if (voidRight)        a.asInstanceOf[Encoder[Where.Concat[A, B]]]
    else {
      val productEnc: Encoder[(A, B)] = a.product(b)
      productEnc.contramap[Where.Concat[A, B]](c2.project)
    }
  }

  /** Wrap a typed Fragment with a literal prefix and suffix string (e.g. `"foo("`, `")"`). Args unchanged. */
  private[sharp] def wrap[A](prefix: String, inner: Fragment[A], suffix: String): Fragment[A] = {
    val parts = List[Either[String, cats.data.State[Int, String]]](Left(prefix)) ++ inner.parts ++ List(Left(suffix))
    Fragment(parts, inner.encoder, Origin.unknown)
  }

  /** Build a `Fragment[Void]` from a static SQL string. Used to lift `raw(...)` into typed-fragment composition. */
  private[sharp] def voidFragment(sql: String): Fragment[Void] =
    Fragment(List(Left(sql)), Void.codec, Origin.unknown)

  /**
   * Lift an `AppliedFragment` to a `Fragment[Void]` by baking its args via `contramap`. Used when bridging
   * structural / pre-applied pieces (subquery results, `whereRaw` payloads) into typed-Args composition.
   */
  private[sharp] def liftAfToVoid(af: AppliedFragment): Fragment[Void] = {
    val srcEnc: Encoder[Any] = af.fragment.encoder.asInstanceOf[Encoder[Any]]
    if (srcEnc eq Void.codec) af.fragment.asInstanceOf[Fragment[Void]]
    else {
      val srcArgs: Any           = af.argument
      val voidEnc: Encoder[Void] = srcEnc.contramap[Void](_ => srcArgs)
      Fragment(af.fragment.parts, voidEnc, Origin.unknown)
    }
  }

}

/**
 * Postgres-side cast: `expr::<type>`. Turns a `TypedExpr[T, A]` into a `TypedExpr[U, A]` (Args unchanged — a cast
 * adds no parameters).
 */
extension [T, A](expr: TypedExpr[T, A]) {

  def cast[U](using pf: PgTypeFor[U]): TypedExpr[U, A] = {
    val castName = skunk.sharp.pg.PgTypes.castName(skunk.sharp.pg.PgTypes.typeOf(pf.codec))
    val parts    = expr.fragment.parts ++ List(Left(s"::$castName"))
    val frag     = Fragment(parts, expr.fragment.encoder, Origin.unknown)
    TypedExpr(frag, pf.codec)
  }

  /** Render the expression with a SQL column alias: `<expr> AS "<name>"`. */
  def as[N <: String & Singleton](name: N): AliasedExpr[T, N, A] = {
    val parts    = expr.fragment.parts ++ List(Left(s""" AS "$name""""))
    val frag     = Fragment(parts, expr.fragment.encoder, Origin.unknown)
    val c        = expr.codec
    new AliasedExpr[T, N, A] {
      val aliasName: N            = name
      val fragment:  Fragment[A]  = frag
      val codec:     Codec[T]     = c
    }
  }

}

/**
 * A [[TypedExpr]] carrying a compile-time-known alias name (`<expr> AS "<N>"`). Produced by `expr.as("name")`.
 */
trait AliasedExpr[T, N <: String & Singleton, Args] extends TypedExpr[T, Args] {
  def aliasName: N
}
