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

## What's in it

**Queries**
- SELECT / INSERT / UPDATE / DELETE / **MERGE** (`WHEN MATCHED` / `NOT MATCHED` / `NOT MATCHED BY SOURCE`) with `RETURNING`.
- JOINs — INNER / LEFT / RIGHT / FULL / CROSS / LATERAL, auto-aliased, with nullability flowing through outer joins.
- WHERE / GROUP BY / HAVING (incl. `ROLLUP` / `CUBE` / `GROUPING SETS`), ORDER BY (`NULLS FIRST/LAST`), LIMIT / OFFSET, `DISTINCT [ON]`, row locking.
- Subqueries (scalar, `IN`, `EXISTS`, `ANY` / `ALL`, correlated), CTEs, window functions, `UNION` / `INTERSECT` / `EXCEPT`.
- `ON CONFLICT … DO NOTHING / DO UPDATE / DO UPDATE FROM EXCLUDED`, `INSERT … FROM SELECT`, `UPDATE … FROM`, `DELETE … USING`.
- Set-returning functions (`generate_series`, `unnest`) as relations.

**Typed parameters**
- `Param[T]` and named `Param.named["x", T]` — a compiled query is a reusable template whose argument type is inferred (`QueryTemplate[(Int, String), Row]`); run it with bound values, or `prepared`. Literals go inline (`age >= 18`).
- A batch of rows as **one** typed parameter: `Pg.unnestRows[Row]("rows")`.
- `allOf` / `anyOf` for runtime lists of predicates; `allOfT` / `anyOfT` for fixed sets with different parameter types.

**Expressions**
- Comparison, `LIKE`, `BETWEEN`, `IN`, `IS [NOT] DISTINCT FROM`, … ; infix arithmetic `+ - * / %` with Postgres's type promotion and date / time / interval arithmetic.
- A large function catalogue on `Pg` (string, numeric, date/time, aggregates, arrays, ranges, …); `CASE WHEN`; `expr"…"` for anything else.
- `@>` / `<@` / `&&` / `||` shared across arrays, ranges, jsonb, hstore, ltree and tsvector.
- Every operator renders parenthesised, so nested expressions mean what they say.

**Postgres types and extensions**
- Tag types for unambiguous codecs (`Varchar[N]`, `Numeric[P, S]`, `Int2/4/8`, …), ranges, arrays.
- Full-text search (`tsvector` / `tsquery`, `@@`, ranking, headlines) and pgvector embeddings (`PgVector[N]`, distance operators, top-k) in core; citext, ltree, hstore, pg_trgm, pgcrypto, fuzzystrmatch.

**Tables and the schema**
- Describe a table from a case class (`Table.of[T]`) or column by column; `.withPrimary` / `.withUnique` / `.withDefault` / `.withGenerated`; `renamed` for partitions and look-alike tables.
- **Schema validation** at boot: columns, types (incl. `varchar(n)` / `numeric(p,s)` / `vector(n)` drift), nullability, generated columns, PK / UNIQUE, required extensions, and declared indexes (any method, operator class, expression, `INCLUDE`, partial).

**What the compiler checks** — column names, operand and value types, nullability, INSERT completeness, generated columns not being written, views not being mutated, UPDATE / DELETE without a WHERE (unless you ask for `updateAll` / `deleteAll`), parameter types. SQL text is assembled from compile-time constants: `.compile` allocates no dynamic SQL fragments for the standard query shapes (a benchmark keeps it that way).

## Modules

- `skunk-sharp-core` — the DSL, full-text search, pgvector and the in-core contribs, and the schema validator.
- `skunk-sharp-iron` — [Iron](https://iltotore.github.io/iron/) refinements (e.g. `String :| MaxLength[256]` maps to `varchar(256)`).
- `skunk-sharp-refined` — [refined](https://github.com/fthomas/refined) refinements.
- `skunk-sharp-circe` — `json` / `jsonb` via circe, with parametric `Jsonb[A]` / `Json[A]` tags and the jsonb operators.
- `skunk-sharp-postgis` — [PostGIS](https://postgis.net/) types and `ST_*` functions on top of skunk-postgis.

## Installation

Published to [GitHub Packages](https://maven.pkg.github.com/ragb/skunk-sharp):

```scala
resolvers += "skunk-sharp @ GitHub Packages" at
  "https://maven.pkg.github.com/ragb/skunk-sharp"

libraryDependencies += "io.github.ragb" %% "skunk-sharp-core" % "0.0.3"
```

GitHub Packages requires authentication even for public packages. See **[Getting Started](docs/docs/getting-started.md)** for the auth setup and the optional modules.

## Examples

Describe your tables once:

```scala
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given // array codecs (for the unnestRows batch below)

case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
case class Post(id: UUID, author_id: UUID, title: String, created_at: OffsetDateTime)

val users = Table.of[User]("users").withPrimary("id").withDefault("id").withUnique("email").withDefault("created_at")
val posts = Table.of[Post]("posts").withPrimary("id").withDefault("id").withDefault("created_at")
```

**Query with typed parameters** — compile once, run with bound values:

```scala
val recentPosts =
  users
    .innerJoin(posts)
    .on(r => r.users.id === r.posts.author_id)
    .where(r => r.users.age >= Param.named["minAge", Int] && r.posts.title.like(Param.named["title", String]))
    .select(r => (email = r.users.email, title = r.posts.title))
    .orderBy(r => r.posts.created_at.desc)
    .limit(20)
    .compile

// SELECT "users"."email", "posts"."title" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."author_id"
// WHERE ("users"."age" >= $1 AND "posts"."title" LIKE $2) ORDER BY "posts"."created_at" DESC LIMIT 20
recentPosts.run(session)((minAge = 18, title = "%skunk%"))   // F[List[(email: String, title: String)]]
```

**Upsert with RETURNING** — `id` and `created_at` have defaults, so they can be left out:

```scala
users
  .insert((email = "ada@example.com", age = 36))
  .onConflict(u => u.email)
  .doUpdateFromExcluded((t, ex) => t.age := ex.age)
  .returning(u => u.id)
  .compile
  .unique(session)                                          // F[UUID]
```

**Sync a batch with MERGE** — the whole batch is one typed parameter:

```scala
case class Incoming(email: String, age: Int)

val sync =
  users
    .merge(Pg.unnestRows[Incoming]("rows").alias("incoming"))
    .on(r => r.users.email === r.incoming.email)
    .whenMatched.update(r => r.users.age := r.incoming.age)
    .whenNotMatched.insert(s => (email = s.email, age = s.age))
    .compile

sync.run(session)((rows = List(Incoming("ada@example.com", 37), Incoming("grace@example.com", 45))))
```

**Check the schema at boot** — fails with a report if a column, type, constraint or declared index drifted:

```scala
SchemaValidator.validateOrRaise(session, users, posts)
```

Misspell a column, compare an `Int` column with a `String`, leave out a required INSERT column or compile a `.delete` without a WHERE, and it doesn't compile. (These examples are compiled in [ReadmeExamplesSuite](modules/core/src/test/scala/skunk/sharp/readme/ReadmeExamplesSuite.scala).)

## Documentation

Full guides live on the **[documentation site](docs/docs/)** — every snippet is type-checked against the library:

- **[Getting started](docs/docs/getting-started.md)** — install, GitHub Packages auth, first query.
- **[Tables & views](docs/docs/tables.md)** — describing relations, tag types, constraints, generated columns, partitions.
- **[Select](docs/docs/select.md)** — WHERE, projections, typed and named parameters, arithmetic, JOINs, subqueries, window functions, CTEs, set operations, row locking.
- **[Insert](docs/docs/insert.md)** — single / batch, `ON CONFLICT`, `RETURNING`, `INSERT … FROM SELECT`.
- **[Update](docs/docs/update.md)** & **[Delete](docs/docs/delete.md)** — staged builders, `… FROM` / `… USING`, `RETURNING`.
- **[Merge](docs/docs/merge.md)** — `MERGE`, batches via `unnestRows`, `RETURNING` with `merge_action()`.
- **[Full-text search](docs/docs/full-text-search.md)** — `tsvector` / `tsquery`, matching, ranking, headlines.
- **[Schema validation](docs/docs/schema-validation.md)** — the boot-time diff, report-only or fail-fast, including declared indexes.
- **[Contrib](docs/docs/contrib.md)** — pgvector, citext, ltree, hstore, pg_trgm, pgcrypto, fuzzystrmatch.
- **[PostGIS](docs/docs/postgis.md)** — spatial types and `ST_*` functions.
- **[Extensibility](docs/docs/extensibility.md)** — your own Postgres types, operators and functions via `extension` methods.

## Roadmap

No scheduled dates — items get picked up when there's a need. See the [open issues](https://github.com/ragb/skunk-sharp/issues); notable ones:

- Better compile-error messages for DSL misuse.
- Compile-time check that bare SELECT columns appear in `GROUP BY` (Postgres catches it at runtime today).
- More contrib modules (HyperLogLog, cube / earthdistance, intarray, …) and Maven Central publishing once the API settles.

## Development

```bash
sbt core/test          # unit + compile-time tests
sbt tests/test         # integration tests (spins up Postgres 18 via testcontainers, runs dumbo migrations)
sbt iron/test          # Iron refinement module
sbt +test              # everything
```

Integration tests depend on Docker being available. Migrations live in [modules/tests/src/test/resources/migrations/](modules/tests/src/test/resources/migrations/).

## License

Apache-2.0. See [LICENSE](LICENSE).
