# Schema Validation

`SchemaValidator` diffs your declared table/view descriptions against `information_schema`
at runtime. Run it at service startup — before the first query — to catch column renames,
type changes, and nullability drift early.

## Usage

```scala mdoc:silent
import skunk.sharp.dsl.*
import java.util.UUID
import java.time.OffsetDateTime

case class User(id: UUID, email: String, age: Int, deleted_at: Option[OffsetDateTime])
case class Post(id: UUID, author_id: UUID, title: String)
case class ActiveUser(id: UUID, email: String)

val users        = Table.of[User]("users").withPrimary("id").withDefault("id").withUnique("email")
val posts        = Table.of[Post]("posts").withPrimary("id").withDefault("id")
val active_users = View.of[ActiveUser]("active_users")
```

```scala mdoc:compile-only
import skunk.sharp.validation.*
import cats.effect.IO

val session: skunk.Session[IO] = null

// Report-only — decide what to do with mismatches
SchemaValidator.validate[IO](session, users, posts, active_users).flatMap { report =>
  if report.isValid then IO.unit
  else IO.println(report.mismatches.map(_.pretty).mkString("\n"))
}

// Fail-fast — raises SchemaValidationException on any mismatch
SchemaValidator.validateOrRaise[IO](session, users, posts)
```

## Mismatch cases

| Case | When it fires |
| --- | --- |
| `RelationMissing` | Table / view not found in `information_schema` |
| `RelationKindMismatch` | Declared as a `Table` but `information_schema` says it is a view (or vice versa) |
| `ColumnMissing` | A declared column is absent from the database |
| `ExtraColumn` | A column exists in the database but is not in the declaration |
| `TypeMismatch` | Declared type differs from DB — including parametric drift (`varchar(256)` vs `varchar(1024)`) |
| `NullabilityMismatch` | Declared `NOT NULL` but DB column is nullable (or vice versa) |
| `PrimaryKeyMissing` / `PrimaryKeyColumnsDiffer` / `ExtraPrimaryKey` | Declared PK set differs from `information_schema.table_constraints` |
| `UniqueConstraintMissing` / `ExtraUniqueConstraint` | Declared UNIQUE constraint not present in the DB (or vice versa) |
| `ExtensionMissing` | A Postgres extension required by a column tag (or supplied via `extraExtensions`) is not in `pg_extension` |

## What it checks

The validator queries `information_schema.tables` and `information_schema.columns` in a
single round-trip per session. For each declared relation it compares:

- Table / view presence and kind
- Column names (declared vs actual)
- Postgres type (reconstructed from `data_type`, `character_maximum_length`,
  `numeric_precision`, `numeric_scale`)
- Nullability

It also checks:

- **Primary key columns** declared via `.withPrimary("col")` — matched as a set against
  `information_schema.table_constraints`.
- **Unique constraints** declared via `.withUnique("col")` (and `.withUniqueIndex(...)`),
  matched by column set so Postgres's auto-generated names don't trip the diff.
- **Postgres extensions** required by declared column tags (citext, ltree, hstore, …) or
  supplied explicitly via `extraExtensions` — looked up in `pg_extension`.

It does **not** check non-unique indexes, foreign keys, check constraints, or default
expressions — those are owned by migrations, not by the DSL.

## Extensions and contrib tags

Tag types in the contrib modules (`Citext`, `LTree`, `Hstore`, …) wire their required
Postgres extension into their `PgTypeFor` instance. The validator collects every column's
required extension (both via `PgTypeFor.requiredExtension` and via the column's
`skunk.data.Type` looked up in `PgTypes.extensionByType`) and unions in any `extraExtensions`
passed by the caller. Missing extensions surface as `Mismatch.ExtensionMissing(name)`.

```scala mdoc:compile-only
import skunk.sharp.validation.*
import skunk.sharp.contrib.pgtrgm.PgTrgm
import skunk.sharp.contrib.pgcrypto.PgCrypto
import cats.effect.IO

val session: skunk.Session[IO] = null

// Function-only contribs (no tag column) need an explicit opt-in
SchemaValidator.validateOrRaise[IO](
  session,
  Seq(users, posts),
  extraExtensions = Set(PgTrgm.RequiredExtension, PgCrypto.RequiredExtension)
)
```

Function-only modules (pgcrypto, fuzzystrmatch, pg_trgm operators on bare `String`) don't
appear in any column's metadata, so the validator can't auto-discover them — pass their
`RequiredExtension` constants via `extraExtensions` explicitly.

## Example: catching drift at boot

```scala mdoc:compile-only
import cats.effect.{IO, Resource}
import skunk.Session
import skunk.sharp.validation.*

def sessionPool: Resource[IO, Session[IO]] = ???

val startup: IO[Unit] =
  sessionPool.use { session =>
    SchemaValidator.validateOrRaise[IO](session, users, posts)
  }
```

Drop this into your `IOApp.run` before starting the HTTP server. If any column has drifted,
`SchemaValidationException` carries the full `ValidationReport` so you can log it and exit.
