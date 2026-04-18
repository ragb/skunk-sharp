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
- `Update.scala` — `users.update.set(c => (c.email := "x", c.age := 30)).where(…).run(session)` — extension on `Table`. `:=` is an extension on `TypedColumn`. `.returning(…)` / `.returningTuple(…)` / `.returningAll` via shared `MutationReturning`.
- `Delete.scala` — `users.delete.where(…).run(session)` — extension on `Table`. Matching `.returning` / `.returningTuple` / `.returningAll`.

## Compile-time SQL goal (design aspiration)

**Non-trivial goal for v0.1+**: compile as much SQL structure as possible at compile time, so only the actual query
parameters (user-supplied values) are emitted at runtime. Today the DSL builds `AppliedFragment`s at runtime by
composing via `|+|`. The target shape:

- Column names, table names, projection lists, operator symbols, `ORDER BY` / `LIMIT` / `OFFSET` literals → known at
  compile time from `Cols` and the lambda AST. Emit them into the static `parts` of a `skunk.Fragment` via a macro.
- Only the user-supplied parameter values flow through encoders at runtime. These are the `AppliedFragment.argument`.
- `inline` + quoted macros let us generate the `Fragment[A]` structure per query type, specialised per `Cols`/lambda,
  so `sbt compile` carries most of the cost and runtime is a tight parameter-binding path.

This is a substantial refactor; the current runtime assembly is easier to maintain and already correct, so we land
features first and revisit assembly strategy once the DSL surface stabilises.

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
