package skunk.sharp

/**
 * `expr"..."` — a typed SQL string interpolator that weaves literal SQL with [[TypedExpr]] interpolations,
 * producing a [[RawExprBuilder]] you commit to a result type via `.as[T]` / `.asCodec(codec)` /
 * `.asCodec(e: TypedExpr[T, ?])`.
 *
 * **Static-by-default**: every interpolation must be a `TypedExpr` — column refs, `Param[T]` (deferred),
 * `lit(v)` (compile-time literal), or `Param.bind(v)` (explicit bake). Bare runtime values are rejected at
 * compile time, mirroring the `=== / := / between / like` operators. This forces the caller to make the
 * binding decision explicit.
 *
 * Each interpolation contributes its `Args` slot to the result; the visible `Args` type is the smart-flat
 * concat over every slot (see `Where.FoldConcat`), matching how every other typed-args builder folds N
 * inputs.
 *
 * Use `.asCodec(e)` (passing one of the spliced TypedExprs) to reuse that expression's exact codec — the
 * common shape for tag-preserving function helpers — instead of writing `.asCodec(e.codec)`.
 *
 * {{{
 *   val ageGte18: TypedExpr[Boolean, Int] =
 *     expr"$ageCol >= ${Param[Int]}".as[Boolean]
 *
 *   val emailLike: TypedExpr[Boolean, Void] =
 *     expr"$emailCol LIKE ${lit("%@example.com")}".as[Boolean]
 *
 *   // Reuse a column's exact codec for tag-preserving function calls
 *   val upperEmail: TypedExpr[String, Void] =
 *     expr"upper($emailCol)".asCodec(emailCol)
 * }}}
 *
 * **Inline-def usage caveat**: Scala 3 `transparent inline` doesn't refine return types in inline-def
 * bodies, so `expr"…"` cannot replace function helpers like `lpad`/`rpad` whose abstract type vars need to
 * thread through the chained `.asCodec(...)` call. Use `expr"…"` in concrete user code; for
 * variadic-typed function helpers in extension modules, build the expression manually (the existing
 * `PgFunction.unary`/`binary`/`naryTypedFold` substrate is the right tool there).
 */
extension (inline sc: StringContext)
  transparent inline def expr(inline args: Any*): RawExprBuilder[?] =
    ${ skunk.sharp.internal.ExprMacro.impl('sc, 'args) }
