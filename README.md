# skunk-sharp

[![Scala 3.8](https://img.shields.io/badge/Scala-3.8-red.svg)](https://www.scala-lang.org/)
[![Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Scala 3 library for **compile-time checked Postgres queries** on top of [skunk](https://typelevel.org/skunk). Describe a table once, then write SELECT / INSERT / UPDATE / DELETE / JOIN statements where column names, operator/value types, nullability, INSERT completeness, and mutability (table vs view) are all verified by the compiler. Validate your table descriptions against a live database at service init.

> :warning: Early development. APIs will change.

## Why?

Skunk gives you correct encoders/decoders and the `sql""` macro, but queries still live as SQL strings with codecs attached separately. If a column is renamed, a comparison targets the wrong type, or a nullable value is treated as non-null, you find out at runtime. skunk-sharp moves those failures into `sbt compile`, and validates that your declared tables still match the database at boot.

## Modules

- `skunk-sharp-core` — the DSL: table/view descriptions, WHERE / ORDER BY / GROUP BY / HAVING / LIMIT / OFFSET, SELECT / INSERT / UPDATE / DELETE, N-way INNER / LEFT / CROSS JOINs with auto-alias, row locking, `ON CONFLICT`, `RETURNING`, aggregates, subqueries (scalar / `IN` / `EXISTS`, correlated or uncorrelated), and the schema validator.
- `skunk-sharp-iron` — optional [Iron](https://iltotore.github.io/iron/) refinement support (e.g. `String :| MaxLength[256]` maps to `varchar(256)`).

## Quick tour

### Describe a table

Two paths, same result:

```scala
import skunk.sharp.*
import skunk.sharp.pg.tags.*

// (a) Derive from a case class. Tag types (`Varchar[N]`, `Int2`, `Numeric[P, S]`, …)
//     pick an unambiguous Postgres codec; bare `String`/`Int` fall back to `text`/`int4`.
case class User(
  id:         UUID,
  email:      Varchar[256],
  age:        Int,
  created_at: OffsetDateTime,
  deleted_at: Option[OffsetDateTime]
)

val users = Table.of[User]("users")
  .withPrimary("id")
  .withUnique("email")
  .withDefault("created_at")

// (b) Column-by-column — no case class needed; the default row type is a named tuple.
val users2 = Table.builder("users")
  .column[UUID]("id", primary = true)
  .column[Varchar[256]]("email", unique = true)
  .column[Int]("age")
  .columnDefaulted[OffsetDateTime]("created_at")
  .columnOpt[OffsetDateTime]("deleted_at")
  .build
```

### Describe a view

```scala
case class ActiveUser(id: UUID, email: Varchar[256], age: Int)
val active = View.of[ActiveUser]("active_users")
// `active.select` works; `active.insert` / `.update` / `.delete` are compile errors.
```

### Build queries

Every builder terminates in `.compile`, which returns a `CompiledQuery[R]` or `CompiledCommand`. Execution methods (`.run`, `.unique`, `.option`, `.stream`, `.cursor`) live as extensions on those.

```scala
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.where.*

// SELECT
users.select
  .where(u => u.age >= 18 && u.email.like("%@example.com"))
  .orderBy(u => u.created_at.desc.nullsLast)
  .limit(20)
  .compile
  .run(session)   // F[List[(id: UUID, email: Varchar[256], age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])]]

// Projections + mapping to a case class
case class UserSnap(email: String, age: Int)
users.select(u => (u.email, u.age)).as[UserSnap].compile.stream(session)

// INSERT — subset named tuple; defaulted columns can be omitted
users.insert((id = UUID.randomUUID(), email = Varchar[256]("a@b"), age = 30, deleted_at = None)).compile.run(session)

// Batch INSERT via any cats `Reducible`
import cats.data.NonEmptyList
users.insert.values(NonEmptyList.of(row1, row2, row3)).compile.run(session)

// ON CONFLICT
users
  .insert(row)
  .onConflict(u => u.id)
  .doUpdateFromExcluded((t, ex) => (t.email := ex.email))
  .compile.run(session)

// UPDATE — staged: `.set` is required before `.where` / `.updateAll`
users.update
  .set(u => (u.email := "new@example.com", u.age := 31))
  .where(u => u.id === someId)
  .compile.run(session)

// DELETE — `.compile` without `.where` or `.deleteAll` is a compile error
users.delete.where(u => u.deleted_at.isNotNull).compile.run(session)

// RETURNING — lifts a command to a CompiledQuery so `.unique` / `.stream` work
users.delete.where(u => u.id === id).returningAll.compile.unique(session)
```

### JOINs

Auto-alias — the alias defaults to the table's name, so `.alias("u")` is optional:

```scala
users
  .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
  .select(r => (r.users.email, r.posts.title))
  .where(r => r.posts.created_at > cutoff)
  .compile.run(session)

// LEFT JOIN flips right-side nullability at the type level.
users
  .leftJoin(posts).on(r => r.users.id ==== r.posts.user_id)
  .select(r => (r.users.email, Pg.count(r.posts.id).as("n")))
  .groupBy(r => r.users.email)
  .compile.run(session)

// CROSS JOIN — no `.on(...)` required. Same for chaining N sources.
users.crossJoin(posts).select(r => (r.users.email, r.posts.title)).compile.run(session)

// Chained after a single-source WHERE, and 3+ source joins:
users.select.where(u => u.age >= 18)
  .innerJoin(posts).on(r => r.users.id ==== r.posts.user_id)
  .leftJoin(tags).on(r => r.posts.id ==== r.tags.post_id)
  .select(r => (r.users.email, r.posts.title, r.tags.name))
  .compile.run(session)

// Explicit aliases still work when the same table appears twice.
users.alias("u1").innerJoin(users.alias("u2")).on(r => r.u1.id ==== r.u2.id)
```

### Subqueries

`TypedExpr`-unified: scalar subqueries, `IN`, and `EXISTS` all take a builder — no inner `.compile` needed, correlation is automatic through lexical scope.

```scala
// Scalar subquery in projection (correlated — inner WHERE closes over outer `u.id`)
users.alias("u").select(u =>
  (u.email, posts.select(_ => Pg.countAll).where(p => p.user_id ==== u.id).asExpr)
).compile.run(session)

// IN subquery
users.select.where(u => u.id.in(posts.select(p => p.user_id))).compile.run(session)

// EXISTS — `Pg.exists` returns a TypedExpr[Boolean] usable directly in WHERE
users.alias("u")
  .select(u => u.email)
  .where(u => u.age >= 18 && Pg.exists(posts.select(_ => lit(1)).where(p => p.user_id ==== u.id)))
  .compile.run(session)
```

### Row locking (tables only)

```scala
users.select.where(u => u.id === id).forUpdate.skipLocked.compile.option(session)
// `.forUpdate` on a View is a compile error.
```

### Compile-time checks

```scala
users.withPrimary("nope")           // ERROR: unknown column
u.age === "not an int"              // ERROR: type mismatch
u.age.isNull                        // ERROR: `isNull` needs a nullable column
u.deleted_at === None               // ERROR: use .isNull instead
u.age.like("%")                     // ERROR: LIKE requires a string column
active.insert(row)                  // ERROR: views are read-only
users.insert((age = 30))            // ERROR: required column `id` is missing
users.update.compile                // ERROR: `.set` has not been called
users.delete.compile                // ERROR: neither `.where` nor `.deleteAll` was called
```

### Validate the schema at boot

```scala
import skunk.sharp.validation.*

// Report-only — decide what to do with mismatches:
SchemaValidator.validate[IO](session, users, active).flatMap { report =>
  if report.isValid then IO.unit
  else IO.println(report.mismatches.map(_.pretty).mkString("\n"))
}

// Fail-fast — raise SchemaValidationException if anything is off:
SchemaValidator.validateOrRaise[IO](session, users, active)
```

`Mismatch` cases: `RelationMissing`, `RelationKindMismatch` (table vs view), `ColumnMissing`, `ExtraColumn`, `TypeMismatch` (including parametric drift like declared `varchar(256)` vs DB `varchar(1024)`), `NullabilityMismatch`.

## Extensibility

Everything operators and functions consume/produce is a `TypedExpr[T]`. Third-party modules add operators by shipping `extension` methods — no changes to core. For example, a hypothetical `skunk-sharp-jsonb` could add:

```scala
opaque type Jsonb = io.circe.Json
given skunk.Codec[Jsonb] = ...
given skunk.sharp.pg.PgTypeFor[Jsonb] = ...

extension (e: skunk.sharp.TypedExpr[Jsonb])
  def ->>(key: String): skunk.sharp.TypedExpr[String] =
    skunk.sharp.PgOperator.infix[Jsonb, String, String]("->>")(e, skunk.sharp.TypedExpr.lit(key))
```

Planned companion modules: `skunk-sharp-json` (based on skunk-circe, for `json`/`jsonb`), `skunk-sharp-refined`, `skunk-sharp-ltree`, `skunk-sharp-arrays`, `skunk-sharp-fts`, eventually `skunk-sharp-postgis`.

## Roadmap

See [CLAUDE.md](CLAUDE.md) for the design notes.

- Window functions (`OVER (…)`).
- CTEs (`WITH …`).
- `UNION` / `INTERSECT` / `EXCEPT`.
- `FULL` / `RIGHT JOIN` (trivial additions to the existing join kinds).
- Compile-time `GROUP BY` coverage check.
- Companion modules for JSON (`skunk-sharp-json`, based on skunk-circe), ltree, arrays, full-text search, and PostGIS.
- Move more of the SQL assembly into macros so only user-supplied parameters flow at runtime.
- Laika + mdoc docs site.

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
