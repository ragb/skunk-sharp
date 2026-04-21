package skunk.sharp

/**
 * Single-import DSL entry point.
 *
 * {{{
 *   import skunk.sharp.dsl.*
 * }}}
 *
 * brings into scope everything needed to use the library:
 *
 *   - Verb entry points: `select`, `insert`, `update`, `delete` (defined in `Select.scala`, `Insert.scala`,
 *     `Update.scala`, `Delete.scala`).
 *   - Structural types: `Table`, `View`, `Column`, `TypedExpr`, `TypedColumn`, `ColumnsView`, `Relation`, `NamedRowOf`,
 *     built-in `Pg` functions, `PgFunction` / `PgOperator` builders.
 *   - WHERE operators and predicate combinators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `in`, `like`, `ilike`, `isNull`,
 *     `isNotNull`, `&&`, `||`, unary `!`), plus `ORDER BY` suffixes (`.asc`, `.desc`) and UPDATE `:=`.
 *
 * Nothing else ought to be imported to write a query.
 */
package object dsl {

  // ---- Types ----
  type Table[Cols <: Tuple, Name <: String & Singleton] = skunk.sharp.Table[Cols, Name]
  val Table: skunk.sharp.Table.type = skunk.sharp.Table

  type View[Cols <: Tuple, Name <: String & Singleton] = skunk.sharp.View[Cols, Name]
  val View: skunk.sharp.View.type = skunk.sharp.View

  type Relation[Cols <: Tuple] = skunk.sharp.Relation[Cols]

  /** Dedicated empty relation — `empty.select(_ => Pg.now)` renders as `SELECT now()`. */
  val empty: skunk.sharp.empty.type = skunk.sharp.empty

  type Column[T, N <: String & Singleton, Null <: Boolean, Attrs <: Tuple] =
    skunk.sharp.Column[T, N, Null, Attrs]

  val ColumnAttr: skunk.sharp.ColumnAttr.type = skunk.sharp.ColumnAttr
  type ColumnAttr = skunk.sharp.ColumnAttr

  type TypedExpr[T] = skunk.sharp.TypedExpr[T]
  val TypedExpr: skunk.sharp.TypedExpr.type = skunk.sharp.TypedExpr

  type AliasedExpr[T, N <: String & Singleton] = skunk.sharp.AliasedExpr[T, N]

  // Extension methods on TypedExpr[T] (cast, as). Exported here so callers that only pull `skunk.sharp.dsl.*` still
  // see them.
  export skunk.sharp.{as, cast}

  type TypedColumn[T, Null <: Boolean, N <: String & Singleton] = skunk.sharp.TypedColumn[T, Null, N]

  type ColumnsView[Cols <: Tuple] = skunk.sharp.ColumnsView[Cols]
  val ColumnsView: skunk.sharp.ColumnsView.type = skunk.sharp.ColumnsView

  type NamedRowOf[Cols <: Tuple]                         = skunk.sharp.NamedRowOf[Cols]
  type NamesOf[Cols <: Tuple]                            = skunk.sharp.NamesOf[Cols]
  type ValuesOf[Cols <: Tuple]                           = skunk.sharp.ValuesOf[Cols]
  type HasColumn[Cols <: Tuple, N <: String & Singleton] = skunk.sharp.HasColumn[Cols, N]

  // ---- Postgres functions / operators ----
  val PgFunction: skunk.sharp.PgFunction.type = skunk.sharp.PgFunction
  val PgOperator: skunk.sharp.PgOperator.type = skunk.sharp.PgOperator
  val Pg: skunk.sharp.Pg.type                 = skunk.sharp.Pg

  // ---- WHERE predicate type and combinators ----
  type Where = skunk.sharp.where.Where
  val Where: skunk.sharp.where.Where.type = skunk.sharp.where.Where

  // The expression-level operators ("WHERE operators" historically, but they produce a plain
  // `TypedExpr[Boolean]` and work anywhere an expression goes — projections, ORDER BY, HAVING, function args).
  export skunk.sharp.ops.{
    !==,
    <,
    <=,
    ===,
    ====,
    >,
    >=,
    between,
    betweenSymmetric,
    gtAll,
    gtAny,
    gteAll,
    gteAny,
    ilike,
    in,
    isDistinctFrom,
    isDistinctFromExpr,
    isNotDistinctFrom,
    isNotDistinctFromExpr,
    isNotNull,
    isNull,
    like,
    ltAll,
    ltAny,
    lteAll,
    lteAny,
    notBetween,
    notSimilarTo,
    similarTo
  }
  export skunk.sharp.ops.Stripped

  // Boolean combinators (`&&`, `||`, `!`, `and`, `or`, `not`) stay in `skunk.sharp.where` — they're about
  // composing predicates, not building them.
  export skunk.sharp.where.{&&, ||, and, not, or}

  // ---- Schema validation ----
  val SchemaValidator: skunk.sharp.validation.SchemaValidator.type = skunk.sharp.validation.SchemaValidator
  type ValidationReport = skunk.sharp.validation.ValidationReport

  val ValidationReport: skunk.sharp.validation.ValidationReport.type =
    skunk.sharp.validation.ValidationReport

  type Mismatch = skunk.sharp.validation.Mismatch
  val Mismatch: skunk.sharp.validation.Mismatch.type = skunk.sharp.validation.Mismatch
  type SchemaValidationException = skunk.sharp.validation.SchemaValidationException

  // ---- Postgres types ----
  // We reuse skunk's [[skunk.data.Type]] directly. `PgTypes` carries the information_schema.data_type mapping used
  // by the schema validator and the SQL cast-name helper used by [[TypedExpr.cast]].
  val PgTypes: skunk.sharp.pg.PgTypes.type = skunk.sharp.pg.PgTypes
  type PgTypeFor[T] = skunk.sharp.pg.PgTypeFor[T]
  val PgTypeFor: skunk.sharp.pg.PgTypeFor.type = skunk.sharp.pg.PgTypeFor

  // ---- Arrays ----
  // `Arr[T]` is skunk's native Postgres-array type; `skunk.sharp.pg.arrays.given` ships `PgTypeFor[Arr[T]]` for
  // primitive element types plus a generic cats `Alternative + Foldable` derivation that covers `List`, `Vector`,
  // `Chain`, `LazyList`, …. `.to[F]` / `.toArr` bridge between skunk's `Arr[T]` and any cats-foldable collection.
  //
  // Array operators (`@>`, `<@`, `&&`, `||`, `= ANY(…)` as `.contains` / `.containedBy` / `.overlaps` / `.concat` /
  // `.elemOf`) and functions live in [[skunk.sharp.pg.ArrayOps]] / [[skunk.sharp.Pg]] — `.contains` / `.containedBy`
  // are intentionally *not* re-exported here because they clash with similarly-named extensions in the circe module
  // (jsonb `@>` / `<@`). Import `skunk.sharp.pg.ArrayOps.*` where array ops are needed.
  type Arr[T] = skunk.data.Arr[T]
  val Arr: skunk.data.Arr.type = skunk.data.Arr
  export skunk.sharp.pg.arrays.given
  export skunk.sharp.pg.arrays.{to, toArr}

  // ---- Value → expression lifting ----
  //
  // Two distinct verbs, each with a specific role:
  //
  //   - `lit(v)` — **compile-time primitive literal only**. `lit(42)` / `lit(true)` renders inline (`42` / `TRUE`)
  //     in the SQL text, no bound parameter. Runtime values and non-primitive types are a compile error — the
  //     message points at the operator paths (which parameterise for you) or at `param` for explicit lifting.
  //
  //   - `param(v)` — **explicit "bind this runtime value as `$N`"**. Use when you need a `TypedExpr[T]` from a
  //     runtime value in a position where no operator extension applies. Rare; most cases go through operators.
  //
  // WHERE operators (`===`, `!==`, `<`, `<=`, `>`, `>=`, `.in(...)`, `.like(...)`, `.ilike(...)`, array
  // `.contains(...)` / `.containedBy(...)` / `.overlaps(...)`, …) take their RHS value directly and parameterise
  // internally. That's the normal path — `lit` / `param` aren't needed there.
  //
  // `inline v: T` on `lit` forwards the compile-time constant through to the macro in
  // [[skunk.sharp.TypedExpr.lit]].
  inline def lit[T](inline v: T)(using pf: PgTypeFor[T]): skunk.sharp.TypedExpr[T] = skunk.sharp.TypedExpr.lit(v)

  def param[T](v: T)(using pf: PgTypeFor[T]): skunk.sharp.TypedExpr[T] = skunk.sharp.TypedExpr.parameterised(v)

}
