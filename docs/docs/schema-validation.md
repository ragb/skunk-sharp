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
| `GeneratedMismatch` | DB column is `GENERATED ALWAYS AS (…)` but not declared `.withGenerated` (or vice versa) |
| `PrimaryKeyMissing` / `PrimaryKeyColumnsDiffer` / `ExtraPrimaryKey` | Declared PK set differs from `information_schema.table_constraints` |
| `UniqueConstraintMissing` / `ExtraUniqueConstraint` | Declared UNIQUE constraint not present in the DB (or vice versa) |
| `ExtensionMissing` | A Postgres extension required by a column tag (or supplied via `extraExtensions`) is not in `pg_extension` |
| `IndexMissing` / `IndexDefinitionMismatch` / `ExtraIndex` | A declared non-unique index is absent or defined differently, or (for a table that declares indexes) an undeclared one exists — see [Non-unique indexes](#non-unique-indexes) |

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

- **Non-unique indexes** declared via `.withIndex` / `.withSortedIndex` / `.withPartialIndex`
  — see [below](#non-unique-indexes).

It does **not** check foreign keys, check constraints, or default expressions — those are
owned by migrations, not by the DSL.

## Non-unique indexes

Indexes a query plan depends on can be declared on the `Table`, so dropping or changing one in
a migration is caught at boot. Declaring is opt-in per table: a table that declares no index
isn't index-checked; once it declares one, every non-unique index on it must be declared.
PK / UNIQUE indexes are covered by the constraint check. Names match the migration's
`CREATE INDEX <name> …`.

```scala mdoc:silent
import skunk.sharp.IndexOrder

case class Tx(id: Long, household_id: UUID, booking_date: java.time.LocalDate, account: Option[String])

val transactions = Table.of[Tx]("transaction")
  .withPrimary("id")
  // CREATE INDEX tx_household_booking_idx ON transaction (household_id, booking_date DESC, id DESC)
  .withSortedIndex["tx_household_booking_idx", ("household_id", "booking_date", "id")](
    (IndexOrder.Asc, IndexOrder.Desc, IndexOrder.Desc)
  )
  // CREATE INDEX tx_account_idx ON transaction (account) WHERE account IS NOT NULL
  .withPartialIndex["tx_account_idx", Tuple1["account"]]("account IS NOT NULL")
  // CREATE INDEX tx_booking_idx ON transaction (booking_date)
  .withIndex["tx_booking_idx", Tuple1["booking_date"]]
```

Column names are checked at compile time, and `withSortedIndex` takes one `IndexOrder` per key
(`Asc`, `Desc`, `AscNullsFirst`, `DescNullsLast`) — a tuple of the wrong arity doesn't compile.

Anything else — another access method, operator classes, collations, expression keys,
`INCLUDE` columns, storage parameters, a standalone `CREATE UNIQUE INDEX` — goes through
`withIndexDef`:

```scala mdoc:silent
import skunk.sharp.{IndexDef, IndexKey}
import skunk.sharp.dsl.given // PgTypeFor for array columns

case class Doc(id: Long, sku: String, name: String, tags: skunk.data.Arr[Int], price: BigDecimal)

val docs = Table.of[Doc]("docs")
  .withPrimary("id")
  // CREATE INDEX docs_tags_gin ON docs USING gin (tags)
  .withIndexDef(IndexDef("docs_tags_gin", IndexKey.column("tags")).withMethod("gin"))
  // CREATE INDEX docs_name_pattern_idx ON docs (name text_pattern_ops)
  .withIndexDef(IndexDef("docs_name_pattern_idx", IndexKey.column("name").opclass("text_pattern_ops")))
  // CREATE INDEX docs_lower_name_idx ON docs (lower(name))
  .withIndexDef(IndexDef("docs_lower_name_idx", IndexKey.expr("lower(name)")))
  // CREATE INDEX docs_price_cover_idx ON docs (price) INCLUDE (name) WITH (fillfactor = 70)
  .withIndexDef(IndexDef("docs_price_cover_idx", IndexKey.column("price")).include("name").withStorage("fillfactor" -> "70"))
  // CREATE UNIQUE INDEX docs_sku_uidx ON docs (sku)
  .withIndexDef(IndexDef("docs_sku_uidx", IndexKey.column("sku")).unique)
```

For pgvector, e.g.
`IndexDef("chunks_embedding_hnsw", IndexKey.column("embedding").opclass("vector_cosine_ops")).withMethod("hnsw")`.

The comparison is against Postgres's own `pg_get_indexdef`, ignoring parentheses, quotes,
spacing and case. Expression keys and predicates are compared in the form Postgres prints them —
it may add casts (`lower((email)::text)` for a `varchar` column), and a mismatch report shows the
database's form to copy. Column keys and `INCLUDE` columns of a `withIndexDef` are checked when
the table is built; expression keys aren't.

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
