# Schema Validation

`SchemaValidator` diffs your declared table/view descriptions against `information_schema`
at runtime. Run it at service startup — before the first query — to catch column renames,
type changes, and nullability drift early.

## Usage

```scala
import skunk.sharp.validation.*
import cats.effect.IO

// session: skunk.Session[IO]

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

## What it checks

The validator queries `information_schema.tables` and `information_schema.columns` in a
single round-trip per session. For each declared relation it compares:

- Table / view presence and kind
- Column names (declared vs actual)
- Postgres type (reconstructed from `data_type`, `character_maximum_length`,
  `numeric_precision`, `numeric_scale`)
- Nullability

It does **not** check indexes, constraints, or default expressions — those are owned by
migrations, not by the DSL.

## Example: catching drift at boot

```scala
import cats.effect.{IO, Resource}
import skunk.Session
import skunk.sharp.validation.*
import natchez.Trace.Implicits.noop

def sessionPool: Resource[IO, Session[IO]] = ???

val startup: IO[Unit] =
  sessionPool.use { session =>
    SchemaValidator.validateOrRaise[IO](session, users, posts)
  }
```

Drop this into your `IOApp.run` before starting the HTTP server. If any column has drifted,
`SchemaValidationException` carries the full `ValidationReport` so you can log it and exit.
