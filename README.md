# skunk-sharp

[![Scala 3.8](https://img.shields.io/badge/Scala-3.8-red.svg)](https://www.scala-lang.org/)
[![Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Scala 3 library for **compile-time checked Postgres queries** on top of [skunk](https://typelevel.org/skunk). Describe a table once, then write SELECT / INSERT / UPDATE / DELETE statements where column names, operator/value types, nullability, and mutability (table vs view) are all verified by the compiler. Validate your table descriptions against a live database at service init.

> :warning: Early development. APIs will change.

## Why?

Skunk gives you correct encoders/decoders and the `sql""` macro, but queries still live as SQL strings with codecs attached separately. If a column is renamed, a comparison targets the wrong type, or a nullable value is treated as non-null, you find out at runtime. skunk-sharp moves those failures into `sbt compile`, and validates that your declared tables still match the database at boot.

## Modules

- `skunk-sharp-core` — the DSL, table/view descriptions, WHERE/ORDER BY/LIMIT/OFFSET, SELECT/INSERT/UPDATE/DELETE, and the schema validator.
- `skunk-sharp-iron` — optional [Iron](https://iltotore.github.io/iron/) refinement support (e.g. `String :| Match["^[^@]+@[^@]+$"]`).

## Quick tour

### Describe a table

Two paths, same result:

```scala
import skunk.sharp.*

// (a) Derive from a case class
case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])

val users = Table.of[User]("users")
  .withPrimary("id")
  .withUnique("email")
  .withDefault("created_at")

// (b) Column-by-column — no case class needed; `Row` becomes a named tuple
val users2 = Table.builder("users")
  .column[UUID]("id", primary = true)
  .column[String]("email", unique = true)
  .column[Int]("age")
  .columnDefaulted[OffsetDateTime]("created_at")
  .columnOpt[OffsetDateTime]("deleted_at")
  .build
```

### Describe a view

```scala
case class ActiveUser(id: UUID, email: String, age: Int)
val active = View.of[ActiveUser]("active_users")
// `active.select` works; `active.insert` / `.update` / `.delete` are compile errors.
```

### Build queries

```scala
import skunk.sharp.*
import skunk.sharp.dsl.*
import skunk.sharp.where.*

// SELECT
users.select
  .where(u => u.age >= 18 && u.email.like("%@example.com"))
  .orderBy(u => u.created_at.desc)
  .limit(20)
  .run(session)   // F[List[(id: UUID, email: String, age: Int, created_at: OffsetDateTime, deleted_at: Option[OffsetDateTime])]]

// INSERT
users.insert((
  id = UUID.randomUUID(),
  email = "a@b",
  age = 30,
  created_at = OffsetDateTime.now(),
  deleted_at = None
)).run(session)

// UPDATE
users.update
  .set(u => (u.email := "new@example.com", u.age := 31))
  .where(u => u.id === someId)
  .run(session)

// DELETE
users.delete.where(u => u.deleted_at.isNotNull).run(session)
```

### Compile-time checks

```scala
users.withPrimary("nope")           // ERROR: unknown column
c.age === "not an int"              // ERROR: type mismatch
c.age.isNull                        // ERROR: `isNull` needs a nullable column
c.deleted_at === None               // ERROR: use .isNull instead
c.age.like("%")                     // ERROR: LIKE requires a string column
active.insert(…) / .update / .delete // ERROR: views are read-only
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

`Mismatch` cases: `RelationMissing`, `RelationKindMismatch` (table vs view), `ColumnMissing`, `ExtraColumn`, `TypeMismatch`, `NullabilityMismatch`.

## Extensibility

Everything operators and functions consume/produce is a `TypedExpr[T]`. Third-party modules add operators by shipping `extension` methods — no changes to core. For example, a hypothetical `skunk-sharp-jsonb` could add:

```scala
opaque type Jsonb = io.circe.Json
given skunk.Codec[Jsonb] = …
given skunk.sharp.pg.PgTypeFor[Jsonb] = …

extension (e: skunk.sharp.TypedExpr[Jsonb])
  def ->>(key: String): skunk.sharp.TypedExpr[String] =
    skunk.sharp.TypedExpr(…, skunk.codec.all.text)
```

Planned companion modules: `skunk-sharp-jsonb`, `skunk-sharp-ltree`, `skunk-sharp-arrays`, `skunk-sharp-fts`.

## Roadmap

See [CLAUDE.md](CLAUDE.md) for the design notes and [docs/](docs/) when docs land. v0.1+ items:

1. Subset-named-tuple INSERT (omit defaulted columns automatically).
2. `RETURNING` across INSERT/UPDATE/DELETE.
3. `GROUP BY` / `HAVING` / `DISTINCT`.
4. JOINs.
5. Subqueries (scalar + `IN`/`EXISTS`) and window functions.
6. Companion modules for JSON, ltree, arrays, full-text search.
7. Laika + mdoc docs site.

## Development

```bash
sbt core/test          # unit + compile-time tests (~300 ms)
sbt tests/test         # integration tests (spins up Postgres 18 via testcontainers, runs dumbo migrations)
sbt iron/test          # Iron refinement module
sbt +test              # everything
```

Integration tests depend on Docker being available. Migrations live in [modules/tests/src/test/resources/migrations/](modules/tests/src/test/resources/migrations/).

## License

Apache-2.0. See [LICENSE](LICENSE).
