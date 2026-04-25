# skunk-sharp — Claude working notes

Compile-time-checked Postgres query DSL on top of [skunk](https://typelevel.org/skunk). Scala 3.8.3, sbt-typelevel, tpolecat flags with `-Werror`. This file orients future Claude sessions; keep it terse.

## Modules

- `modules/core` — the DSL (published artefact `skunk-sharp-core`).
- `modules/iron` — optional Iron refinement support (`skunk-sharp-iron`). Depends on core.
- `modules/tests` — integration tests; not published. Uses testcontainers-postgresql and **dumbo** for migrations (see [modules/tests/src/test/resources/migrations/](modules/tests/src/test/resources/migrations/)).

## Key design decisions

- **Scala 3.8.3**, not 3.3 LTS. Chosen to use named tuples (stable in 3.7+).
- **No `Row` type parameter on `Table`/`View`.** A relation is `Table[Cols]` / `View[Cols]`; projections are per-operation (default = named tuple `NamedRowOf[Cols]`, override with `.as[T]`).
- **Table vs View** live as siblings of a common [`Relation[Cols]`](modules/core/src/main/scala/skunk/sharp/Relation.scala). `.select` is an extension on `Relation`; `.insert`/`.update`/`.delete` are extensions on `Table` only — mutating a `View` is a compile error.
- **`PgType` is info-only.** We do NOT generate DDL. The field `dataType` matches `information_schema.columns.data_type` for the schema validator; that's all.
- **skunk 1.0.0** — pulled `AppliedFragment`, `Session.Builder`, otel4s-based tracing from the latest API.
- **The DSL is extensible by design.** Everything operators/functions produce is a `TypedExpr[T]`; third-party modules (jsonb, ltree, arrays, PostGIS, user functions) add operators by shipping `extension` methods on `TypedExpr[T]` — no core changes required. See [TypedExpr](modules/core/src/main/scala/skunk/sharp/TypedExpr.scala).

## Type tags — unambiguous codec selection

[`skunk.sharp.pg.tags`](modules/core/src/main/scala/skunk/sharp/pg/tags.scala) ships opaque subtype aliases (`Varchar[N]`, `Bpchar[N]`, `Text`, `Int2/4/8`, `Numeric[P, S]`) each with a `given PgTypeFor[…]`. Declaring a case-class field as `Varchar[256]` (instead of bare `String`) makes `Table.of[T]` pick `varchar(256)` without ambiguity. Tags are `<: Base`, so values flow as plain `String` / `Int` / `Long` at runtime; the tag exists only for typeclass resolution. Construct via the companion `apply` (`Varchar[256]("x")`) — we deliberately don't ship `Conversion` givens because Scala's implicit-conversion language import is intrusive.

[`TableBuilder.column[T]`](modules/core/src/main/scala/skunk/sharp/TableBuilder.scala) (continuation pattern: `ColumnCont` / `OptColumnCont`) accepts a tag type and resolves the codec via `PgTypeFor[T]`. Explicit-codec `.column("name", codec)` stays — users pick per column.

The iron module [bridges common constraints to core tags](modules/iron/src/main/scala/skunk/sharp/iron/package.scala): `String :| MaxLength[N]` routes to `Varchar[N]`, `String :| FixedLength[N]` to `Bpchar[N]`. Given-resolution prefers the specific bridge over the generic `refinedPgTypeFor[A, C]` fallback.

The [schema validator](modules/core/src/main/scala/skunk/sharp/validation/SchemaValidator.scala) reconstructs the actual Postgres type from `data_type` + `character_maximum_length` + `numeric_precision` + `numeric_scale`, then compares to the declared `skunk.data.Type`'s `.name`. Parametric drift (declared `varchar(256)` vs DB `varchar(1024)`) is caught as a `TypeMismatch`.

## Column and table shape

- [`Column[T, N <: String & Singleton, Null <: Boolean, Default <: Boolean]`](modules/core/src/main/scala/skunk/sharp/Column.scala) — phantom-typed column descriptor. Name is a singleton so match types can look it up at compile time.
- [`HasColumn`/`ColumnAt`/`ColumnType`/`ColumnNullable`](modules/core/src/main/scala/skunk/sharp/HasColumn.scala) — match types over `Cols` tuples.
- [`NamedRowOf[Cols]`](modules/core/src/main/scala/skunk/sharp/TableBuilder.scala) — the default Scala 3 named-tuple row type.
- Two paths to build a `Table`:
  - [`Table.of[T <: Product]("name")`](modules/core/src/main/scala/skunk/sharp/Table.scala): derive columns from a case class `Mirror.ProductOf`.
  - [`Table.builder("name").column[T]("n")…`](modules/core/src/main/scala/skunk/sharp/TableBuilder.scala): column-by-column, no case class needed. Two-step continuation pattern (`column[T](using pf)` returns a `ColumnCont` whose `.apply[N](n)` takes the literal name) works around Scala's all-or-nothing type inference.
- `View` mirrors this with [`View.of`](modules/core/src/main/scala/skunk/sharp/View.scala) / `View.builder` and a `ViewBuilder`.
- Constraint modifiers `.withPrimary("n")`, `.withUnique("n")` (term-level flags) and `.withDefault("n")` (flips the `Default` phantom) live on `Table`. All verify column existence at compile time via `HasColumn[Cols, N] =:= true`.

## WHERE DSL

- [`TypedExpr[T]`](modules/core/src/main/scala/skunk/sharp/TypedExpr.scala): `render: AppliedFragment` + `codec: Codec[T]`. `TypedExpr.lit(v)` for bound-param literals, `TypedExpr.raw(sql)` for raw SQL bits, `TypedExpr.joined(parts, sep)` for `", "`-style joining.
- [`TypedColumn[T, Null]`](modules/core/src/main/scala/skunk/sharp/TypedColumn.scala): leaf `TypedExpr`.
- [`Where`](modules/core/src/main/scala/skunk/sharp/where/Where.scala): thin wrapper around `TypedExpr[Boolean]`; has `.and`/`&&`, `.or`/`||`, `.not`/`unary_!`. Open by design — any third-party `TypedExpr[Boolean]` slots in via `Where(expr)`.
- [`where/Ops.scala`](modules/core/src/main/scala/skunk/sharp/where/Ops.scala): v0 operators as `extension` methods — `===, !==, <, <=, >, >=, in, like, ilike, isNull, isNotNull`. Ordering ops require `cats.Order[T]`. `isNull`/`isNotNull` gated to `TypedColumn[T, true]`.
- [`ColumnsView[Cols]`](modules/core/src/main/scala/skunk/sharp/ColumnsView.scala): the named-tuple typed view passed to WHERE/ORDER BY/SET lambdas. `cols.<columnName>` resolves to `TypedColumn[T, Null]` via Scala 3 named-tuple Selectable support.

## DSL builders

All under [modules/core/src/main/scala/skunk/sharp/dsl/](modules/core/src/main/scala/skunk/sharp/dsl/). **All entry points are top-level** (no `table.method` forms):

**Consistent shape: every verb lives on the relation.** `users.select` / `users.insert` / `users.update` / `users.delete`. Views get `.select` only (mutations are compile errors). FROM-less queries use the dedicated `empty` relation (`empty.select(_ => Pg.now)`).

- `Select.scala` — SELECT is an extension on `Relation` (works for Tables and Views).
  - `users.select` → `SelectBuilder[Cols]` (whole row). Chain `.where`, `.orderBy`, `.limit`, `.offset`, `.distinctRows`.
  - Row locking: `.forUpdate` / `.forShare` / `.forNoKeyUpdate` / `.forKeyShare`, plus `.skipLocked` / `.noWait`.
  - `users.select(u => u.email)` / `users.select(u => (u.email, u.age))` — function-form projection.
  - `users.select(u => (u.email, u.age)).as[Snap]` — map to a case class via `Mirror`.
  - `empty.select(_ => Pg.now)` — FROM-less via the special `empty: Relation[EmptyTuple]` singleton. Its `hasFromClause` is `false`, so the compiler elides the `FROM` clause.
  - `.distinctRows` renders `SELECT DISTINCT …`.
- `Insert.scala` — INSERT is an extension on `Table` only (views reject at compile time).
  - `users.insert(row)` — `row` is any named tuple whose field names are a subset of the table's columns. The subset must cover every required (non-defaulted) column; omitted `.withDefault`ed columns are filled in by Postgres (sequence PKs, `DEFAULT now()` timestamps, …). Three compile-time checks: all names exist (`AllNamesInCols`), all required are present (`CoversRequired`), and each value's type matches the column's declared Scala type.
  - `users.insert.values(rows)` — batch; `rows` is any `cats.Reducible` (`NonEmptyList`, `NonEmptyVector`, `NonEmptyChain`, `NonEmptySeq`, …). "At least one row" is a type-level guarantee.
  - The varargs `.values(r, more*)` is sugar for the single- or multi-row case when you already have the rows spelled out.
  - `.returning(c => c.id)` / `.returningTuple(…)` / `.returningAll` — append `RETURNING`.
  - `.onConflictDoNothing` / `.onConflict(c => c.id).doNothing` / `.onConflict(c => c.id).doUpdate(c => (c.email := "x"))`.
  - `.onConflict(c => c.id).doUpdateFromExcluded((t, ex) => (t.email := ex.email))` — access the incoming row via Postgres's `excluded.<col>` pseudo-table. [`TypedColumn.qualified`](modules/core/src/main/scala/skunk/sharp/TypedColumn.scala) and [`ColumnsView.qualified`](modules/core/src/main/scala/skunk/sharp/ColumnsView.scala) produce columns rendered with an arbitrary prefix.
- `Update.scala` — extension on `Table`. **Staged state machine**: `users.update` → `UpdateBuilder` (has only `.set`) → `.set(...)` → `UpdateWithSet` (has only `.where` and `.updateAll`) → either `.where(...)` or `.updateAll` → `UpdateReady` (has `.compile`, `.returning*`, chained `.where`). Calling `.compile` without a WHERE or an explicit `.updateAll` is a compile error — the method simply doesn't exist on `UpdateWithSet`. `:=` is an extension on `TypedColumn`.
- `Delete.scala` — extension on `Table`. **Staged state machine**: `users.delete` → `DeleteBuilder` (has only `.where` and `.deleteAll`) → either `.where(...)` or `.deleteAll` → `DeleteReady` (`.compile`, `.returning*`, chained `.where`). `users.delete.compile` without a WHERE or explicit `.deleteAll` does not compile.

**Select locking is gated to Tables.** `SelectBuilder[R <: Relation[Cols], Cols]` and `ProjectedSelect[R, Cols, Row]` thread the relation type through so `.forUpdate` / `.forShare` / `.forNoKeyUpdate` / `.forKeyShare` / `.skipLocked` / `.noWait` require `R <:< Table[Cols]` via an implicit `<:<` evidence. Calling them on a `View` is a compile error — Postgres would reject them at runtime anyway.

`.offset(n)` without a prior `.limit(n)` is **allowed** — Postgres supports it per SQL:2008, and we don't lint valid SQL at the type level.

## GROUP BY, HAVING, aggregates

Aggregates live on `Pg` in [PgFunction.scala](modules/core/src/main/scala/skunk/sharp/PgFunction.scala): `countAll`, `count`, `countDistinct`, `sum`, `avg`, `min`, `max`, `stringAgg`, `boolAnd`, `boolOr`. Every aggregate returns a `TypedExpr[R]`, so they slot into SELECT projections, HAVING predicates, or ORDER BY expressions anywhere a `TypedExpr` is expected.

`sum` and `avg` use typeclasses (`SumOut[I]`, `AvgOut[I]`) that map the input Scala type to Postgres's actual result type — `sum(Int)` → `Long`, `sum(Long)` → `BigDecimal`, `sum(Double)` → `Double`, `avg(Int)` → `BigDecimal`, `avg(Double)` → `Double`. Matches what Postgres actually returns so the decoded value aligns.

`.groupBy(cols => …)` and `.having(cols => …)` live on both `SelectBuilder` and `ProjectedSelect`. Rendering order: `SELECT … FROM … WHERE … GROUP BY … HAVING … ORDER BY … LIMIT … OFFSET … LOCKING`. Compile-time enforcement that all bare SELECT columns appear in GROUP BY is a roadmap item (requires `TypedColumn` to carry its singleton column-name type param); today, Postgres raises the misalignment at runtime.

## Builders split from execution: `CompiledQuery` / `CompiledCommand`

Every builder's `.compile` returns one of two types — defined in [Compiled.scala](modules/core/src/main/scala/skunk/sharp/dsl/Compiled.scala):

- `CompiledQuery[Args, R]` — for SELECT, `RETURNING`-variants. Extensions: `.run`, `.unique`, `.option`, `.stream`, `.cursor`, `.prepared`.
- `CompiledCommand[Args]` — for INSERT/UPDATE/DELETE without RETURNING. Extensions: `.run`, `.prepared`.

`Args` is the captured-parameter tuple — visible at the call site for typed builder chains. A `users.select.where(u => u.age >= 18 && u.email === "x").compile` produces `CompiledQuery[(Int, String), NamedRow]`; users can ascribe the type, build the query once, and re-bind values via `q.prepared(session): F[PreparedQuery[F, Args, R]]`.

The fragment is held as `Fragment[Args]` + `args: Args` internally; `.af: AppliedFragment` is a derived `lazy val` for subquery embedding.

All session-facing operations live as `inline` extensions on these two types, defined once — so every verb (including `RETURNING` variants) shares the full [`skunk.Session`](https://github.com/typelevel/skunk) execution surface. Usage: `users.select.where(…).compile.stream(session)`, `users.delete.where(…).compile.run(session)`, `users.insert(…).returning(u => u.id).compile.unique(session)`.

## `Args` threading — typed captured parameters

Every builder threads two captured-args type parameters end-to-end:

- `SelectBuilder[Ss, WArgs, HArgs]` — `WArgs` accumulates from `.where(_ => Where[A])` lambdas, `HArgs` from `.having(_ => Where[H])`. Other operations (`.orderBy`, `.limit`, `.offset`, `.distinctRows`, locking, `.groupBy`) leave them unchanged.
- `ProjectedSelect[Ss, Proj, Groups, WArgs, HArgs, Row]` — same `WArgs` / `HArgs` story.
- `DeleteReady[Cols, Name, Args]` — `Args` from `.where(_ => Where[A])`.
- `UpdateReady[Cols, Name, SetArgs, WArgs]` — `SetArgs` from `.set(_ => col := value)` (single-col), widened to `Any` for tuple-form `.set` and `.patch`.
- `InsertCommand[Cols, Args]` — `Args` is the row-values tuple for single-row insert; widened to `?` for `.values(...)` batch and `.from(query)`.

`.compile` collapses the parameter slots through the `Where.Concat[A, B]` match type, which drops `skunk.Void` placeholders so a no-WHERE / no-HAVING query is `CompiledQuery[Void, R]`, a WHERE-only query is `CompiledQuery[WArgs, R]`, and so on. Combined predicates pair via raw tuples — `Where[A] && Where[B]` is `Where[(A, B)]`, no normalisation.

**Operators** in [`skunk.sharp.ops`](modules/core/src/main/scala/skunk/sharp/ops/ExprOps.scala) (`===`, `>=`, `between`, `like`, `in`, …) all return `Where[Stripped[T]]` — typed `Args`, never existential. The `SqlMacros.infix` / `infix2` / `postfix` paths bake the LHS column reference + op symbol into a constant `Fragment[Args]` when LHS is a `TypedColumn`; for non-`TypedColumn` LHS (e.g. `Pg.lower(col)`, `col.cast[…]`) the macro renders the LHS to `AppliedFragment` and uses `Encoder.contramap` to bake those existential args while still surfacing the typed RHS `Args` at the result.

**Escape hatches** for genuinely runtime-built predicates:

- `.whereRaw(af: AppliedFragment)` / `.havingRaw(af)` on `SelectBuilder`/`ProjectedSelect`/mutation builders — accept a pre-applied `AppliedFragment` (any third-party fragment, dynamic filter list, etc.) and widen the corresponding slot's `Args` to `?`. The fragment's bound values are baked via contramap; subsequent typed `.where`/`.having` calls compose normally.
- `Where(typedExpr: TypedExpr[Boolean])` — lift any boolean-typed `TypedExpr` (subquery `Pg.exists(...)`, jsonb `@>`, range `<<`, …) to `Where[Void]`. Used by extension modules whose operators don't ship typed `Args`.

## Compile-time SQL goal (design aspiration)

**Non-trivial goal for v0.1+**: compile as much SQL structure as possible at compile time, so only the actual query
parameters (user-supplied values) are emitted at runtime.

**Done so far** (on `macro-sql-assembly`):
- `WHERE`-operator macro baking — every `===`, `>=`, `between`, `in`, `like`, `isNull`, … with a `TypedColumn` LHS produces a single `skunk.Fragment` whose `parts` is compile-time-constant strings.
- Structural-token cache (`SELECT `, ` FROM `, ` WHERE `, `, `, `(`, ` AND `, ` GROUP BY `, ` HAVING `, ` ORDER BY `, …) — process-wide-shared `AppliedFragment`s, returned by `TypedExpr.raw` for the hot strings.
- `lazy val` caching on `TypedColumn.render`, `Table.columnsView`, `Table.deleteFromHeader`, `Table.updateSetHeader`.
- `CompiledQuery[Args, R]` / `CompiledCommand[Args]` carry `Fragment[Args]` + `args` — typed throughout.
- All builders thread `Args` end-to-end (this file's *Args threading* section).

Measured: ~27% faster `.compile` than `main`, ~3-7× less heap allocation per call. `CompileBench` in `modules/core/src/test/scala/skunk/sharp/bench/CompileBench.scala`.

**Still to do** (Phase 5 / issue #24's acid test):
- Bake projection lists, `ORDER BY` direction keywords, `LIMIT`/`OFFSET` literal forms.
- A macro that owns the entire builder chain so the fully-static query case (`users.select.where(u => u.id === lit(uuid)).compile`) collapses to a single interned `Fragment[Void]` at expansion time. Today each leaf bakes its own `parts` list and they concatenate at runtime.
- Compile-time assertion that fully-static queries are `Fragment` constants (the negative twin: dynamic queries must NOT collapse).

## Schema validation

[`SchemaValidator.validate(session, rels…)`](modules/core/src/main/scala/skunk/sharp/validation/SchemaValidator.scala) returns a [`ValidationReport`](modules/core/src/main/scala/skunk/sharp/validation/ValidationReport.scala) of `Mismatch` cases (`RelationMissing`, `RelationKindMismatch`, `ColumnMissing`, `ExtraColumn`, `TypeMismatch`, `NullabilityMismatch`). Queries `information_schema.tables` + `information_schema.columns`. `validateOrRaise(…)` fails with `SchemaValidationException` carrying the report.

## Build

- `build.sbt`: sbt-typelevel 0.8.5, sbt 1.12.9, `tlFatalWarnings := true` — **compile must be clean, no warnings**.
- Versions pinned in one block near the top of `build.sbt` (skunk 1.0.0, cats-effect 3.7.0, munit 1.2.4, munit-cats-effect 2.1.0, Iron 3.0.2, testcontainers-scala 0.41.0, dumbo 0.8.1, otel4s 0.16.0).
- Common sbt commands:
  - `sbt core/test` — unit tests only (~250 ms).
  - `sbt tests/test` — integration tests (spins up a Postgres container per suite, runs dumbo migrations from `modules/tests/src/test/resources/migrations/`).
  - `sbt +test` — everything.
  - `SBT_TPOLECAT_CI=1 sbt compile` — CI-mode flags (`-Werror` plus stricter).

## Working conventions

- **Never** regenerate DDL from our types. The database schema is owned by migrations (dumbo). We only *read* it for validation.
- Every DSL feature must render to `AppliedFragment` via `TypedExpr.render` — no side channels. Extensions hook in through the same interface.
- Tests that render SQL compare against `.fragment.sql` exact strings; whitespace is load-bearing, keep it consistent.
- Prefer `extension` methods on `TypedExpr[T]` over adding methods to `TypedExpr` itself — that's what keeps the surface extensible.
- When adding a new module (iron, jsonb, ltree, …), ship: a `PgTypeFor[MyType]` instance, codec, and extension methods. That's it. Do not touch core.

## Planned roadmap (v0.1+)

1. Multi-source SELECT for JOINs — `Select.from(a, b)` producing a two-source builder; gradual rollout (INNER → LEFT/RIGHT → FULL).
2. `GROUP BY` / `HAVING`.
3. Subqueries (scalar + `IN`/`EXISTS`).
4. Window functions (`OVER (…)`).
5. ORDER BY `NULLS FIRST / LAST`.
6. Companion modules:
   - `skunk-sharp-json` — **based on skunk-circe**, covering `json` and `jsonb` columns with typed operators (`->`, `->>`, `@>`, `?`, …).
   - `skunk-sharp-refined` — companion to `skunk-sharp-iron`, using [eu.timepit.refined](https://github.com/fthomas/refined) for users on the pre-Iron stack.
   - `skunk-sharp-ltree`, `skunk-sharp-arrays`, `skunk-sharp-fts` (full-text search), and eventually PostGIS via `skunk-sharp-postgis`.
7. Docs site via `sbt-typelevel-site` (Laika) with [**mdoc**](https://scalameta.org/mdoc/) — mdoc is the Typelevel-ecosystem replacement for the old `tut` tool; it type-checks every Scala snippet in the markdown against the live library, so examples can't rot. Published to GitHub Pages via the plugin.
