# skunk-sharp

[![Scala 3.8](https://img.shields.io/badge/Scala-3.8-red.svg)](https://www.scala-lang.org/)
[![Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Scala 3 library for **compile-time checked Postgres queries** on top of [skunk](https://typelevel.org/skunk). Describe a table once, then write SELECT / INSERT / UPDATE / DELETE / JOIN statements where column names, operator/value types, nullability, INSERT completeness, and mutability (table vs view) are all verified by the compiler. Validate your table descriptions against a live database at service init.

> :warning: Early development. APIs will change. **This is mostly an experiment** — see the scope below.

## Scope: this is not a SQL replacement

**skunk-sharp does not replace SQL.** It is a *type-safer* way to write a subset of common SQL on top of skunk, and nothing more:

- **SQL is the target.** The DSL mirrors the SQL you'd otherwise hand-write. If you already know SQL, skunk-sharp reads like SQL. If you want a query abstraction that hides SQL, look elsewhere — this isn't that.
- **This won't cover every SQL feature, and it shouldn't try.** Postgres's SQL surface is vast. Attempting 100% coverage through a typed DSL would either mean a mountain of machinery nobody needs or a leaky abstraction that lies about what Postgres does. Neither is worth shipping.
- **If you need the full flexibility of SQL, use skunk's `sql"…"` directly.** That interpolator is the ground truth. skunk-sharp covers the queries that are repetitive, mechanical, and easy to get wrong in string form (typo in a column name, comparison against the wrong type, forgetting a required column in an INSERT, nullable mishandled as non-null). Anything beyond that — recursive CTEs with clever tricks, window functions with custom frames, obscure `plpgsql`, server-side procedural code — belongs in raw SQL.
- **Mixed use is fine and expected.** Your codebase can have skunk-sharp queries for the common cases and raw `sql"…"` queries for the complex ones. They share the same session; they share the same codecs. There's no lock-in either way.

## Goals and non-goals

**Goals:**

- A **type-safer** way to write the *common* subset of SQL on top of skunk. Column names, operator/value types, nullability, INSERT completeness, mutation vs read-only (Table vs View), locking scope — all checked by the compiler.
- **Schema validation** against a live database at service init — `information_schema` diff, report-only or fail-fast.
- **Scala 3 only.** Deliberately leaning into the modern type system: match types, opaque tags with upper bounds, extension methods, `inline`, named tuples, polymorphic function types.
- **Compile-time as much as possible** — but not at any cost. Low-level macro wizardry is avoided when a match type + `inline` reads well enough. Macros are the last resort, not the first.
- **AI-assisted delivery.** Function catalogues, operator sets, mechanical rewrites across modules — these are the kind of work an LLM is good at and a human is slow at. The design decisions stay with the human; the busywork doesn't.
- **Extensible where it's cheap.** The DSL's vocabulary is `TypedExpr[T]`; third-party modules add operators and functions via `extension` methods, tags, and mixin traits (`Pg` is a stack of `PgNumeric`/`PgString`/… — users can swap in their own bundle).
- **Postgres-only, skunk-only.** No pretence of multi-backend support. Postgres is rich enough and skunk is good enough that abstracting doesn't earn its keep.

**Non-goals:**

- **Not replacing SQL.** Not an ORM. Not a query abstraction that hides the SQL shape. Drop to `sql"…"` whenever skunk-sharp doesn't fit.
- **Not full SQL coverage.** Niche / rarely-used features stay in `sql"…"`. The DSL targets the common path.
- **Not more than SQL — no DDL.** Schema is owned by migrations (dumbo, Flyway, whatever). We validate against it; we don't generate it.
- **No query optimisation of any kind.** skunk-sharp translates a typed builder to the corresponding SQL, nothing more — no rewrite passes, no predicate push-down, no join reordering, no hint injection. The Postgres planner is in charge. If a rendered query is slow, the fix is in the query you wrote (or in an `EXPLAIN` + index you're missing), not in anything we'll do to the tree.
- **No speculative extension points.** We add extension hooks when a concrete module needs one (jsonb, ltree, arrays), not to "support a future that isn't now". Sealed stays sealed until an actual use case shows up.
- **Not cross-database.** No MySQL, no SQLite, no H2.

## Modules

- `skunk-sharp-core` — the DSL: table/view descriptions, WHERE / ORDER BY / GROUP BY / HAVING / LIMIT / OFFSET, SELECT / INSERT / UPDATE / DELETE, N-way INNER / LEFT / RIGHT / FULL / CROSS / LATERAL JOINs with auto-alias, row locking, `ON CONFLICT` (`DO NOTHING` / `DO UPDATE` / `DO UPDATE … FROM EXCLUDED`), `RETURNING`, `UPDATE … FROM` / `DELETE … USING`, `INSERT … FROM SELECT`, aggregates with `GROUP BY` / `HAVING` (incl. `ROLLUP` / `CUBE` / `GROUPING SETS`), window functions (`OVER (…)`), CTEs (`WITH …`), set operations (`UNION` / `INTERSECT` / `EXCEPT`, with `ALL`), set-returning functions (`generate_series`, `unnest`), subqueries (scalar / `IN` / `EXISTS` / `ANY` / `ALL`, correlated or uncorrelated), in-core Postgres-extension contribs (citext, ltree, hstore, pg_trgm, pgcrypto, fuzzystrmatch), and the schema validator.
- `skunk-sharp-iron` — optional [Iron](https://iltotore.github.io/iron/) refinement support (e.g. `String :| MaxLength[256]` maps to `varchar(256)`).
- `skunk-sharp-refined` — optional [refined](https://github.com/fthomas/refined) refinement support.
- `skunk-sharp-circe` — Postgres `json` / `jsonb` via [skunk-circe](https://typelevel.org/skunk), with parametric `Jsonb[A]` / `Json[A]` tags that round-trip typed case classes.
- `skunk-sharp-postgis` — [PostGIS](https://postgis.net/) spatial types and `ST_*` operators on top of [skunk-postgis](https://github.com/typelevel/skunk/tree/main/modules/postgis).

## Installation

Published to [GitHub Packages](https://maven.pkg.github.com/ragb/skunk-sharp):

```scala
resolvers += "skunk-sharp @ GitHub Packages" at
  "https://maven.pkg.github.com/ragb/skunk-sharp"

libraryDependencies += "io.github.ragb" %% "skunk-sharp-core" % "<version>"
```

GitHub Packages requires authentication even for public packages, and there are optional modules (`-iron`, `-refined`, `-circe`, `-postgis`). See **[Getting Started](docs/docs/getting-started.md)** for the auth setup and the full dependency list.

## A taste

```scala
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.pg.tags.*

case class User(id: UUID, email: Varchar[256], age: Int, deleted_at: Option[OffsetDateTime])

val users = Table.of[User]("users").withPrimary("id").withUnique("email")

users.select
  .where(u => u.age >= 18 && u.email.like("%@example.com"))
  .orderBy(u => u.age.desc)
  .limit(20)
  .compile
  .run(session)   // F[List[(id: UUID, email: Varchar[256], age: Int, deleted_at: Option[OffsetDateTime])]]
```

Column names, value types, nullability, INSERT completeness, and table-vs-view mutability are all checked by the compiler — a typo or a type mismatch is a compile error, not a runtime surprise.

## Documentation

Full guides live on the **[documentation site](docs/docs/)** — every snippet is type-checked against the live library at build time:

- **[Getting started](docs/docs/getting-started.md)** — install, GitHub Packages auth, first query.
- **[Tables & views](docs/docs/tables.md)** — describing relations, tag types, constraints, the `Table.of` / `Table.builder` paths.
- **[Select](docs/docs/select.md)** — WHERE, projections, ORDER BY, GROUP BY / HAVING, JOINs (INNER / LEFT / RIGHT / FULL / CROSS / LATERAL), subqueries, window functions, CTEs, set operations, row locking.
- **[Insert](docs/docs/insert.md)** — single / batch, `ON CONFLICT`, `RETURNING`, `INSERT … FROM SELECT`.
- **[Update](docs/docs/update.md)** & **[Delete](docs/docs/delete.md)** — staged builders, `… FROM` / `… USING`, `RETURNING`.
- **[Schema validation](docs/docs/schema-validation.md)** — diff declared tables against `information_schema` at boot, report-only or fail-fast.
- **[Contrib modules](docs/docs/contrib.md)** — in-core citext, ltree, hstore, pg_trgm, pgcrypto, fuzzystrmatch.
- **[PostGIS](docs/docs/postgis.md)** — spatial types and `ST_*` operators.
- **[Extensibility](docs/docs/extensibility.md)** — add your own Postgres types and operators via `extension` methods, no core changes.

## Roadmap

The common SELECT / INSERT / UPDATE / DELETE / JOIN surface — including window functions, CTEs,
set operations (`UNION` / `INTERSECT` / `EXCEPT`), `FULL` / `RIGHT` / `LATERAL` joins, set-returning
functions, `ON CONFLICT`, `RETURNING`, `UPDATE … FROM` / `DELETE … USING`, the iron / refined / circe /
postgis modules, and the Laika + mdoc docs site — has shipped. See [CLAUDE.md](CLAUDE.md) for the design
notes and [the docs site](docs/docs/) for usage.

Open items (no scheduled date — picked up when motivation arrives):

- Compile-time enforcement that all bare SELECT columns appear in `GROUP BY` (currently caught at runtime by Postgres).
- Companion modules: `skunk-sharp-fts` (full-text search), arrays, broader PostGIS coverage.
- Owner-macro that collapses a structurally-static query to a single interned `Fragment[Args]` (research item — see CLAUDE.md).

## Development

```bash
sbt core/test          # unit + compile-time tests (~250 ms)
sbt tests/test         # integration tests (spins up Postgres 18 via testcontainers, runs dumbo migrations)
sbt iron/test          # Iron refinement module
sbt +test              # everything
```

Integration tests depend on Docker being available. Migrations live in [modules/tests/src/test/resources/migrations/](modules/tests/src/test/resources/migrations/).

## License

Apache-2.0. See [LICENSE](LICENSE).
