# INSERT

## Single row

Pass a named tuple whose fields are a subset of the table's columns. The compiler
verifies:

1. Every field name exists in the table.
2. Every non-defaulted column is present.
3. Each value's type matches the column's declared Scala type.

```scala mdoc:silent
import skunk.sharp.dsl.*
import java.util.UUID
import java.time.OffsetDateTime

case class User(
  id:         UUID,
  email:      String,
  age:        Int,
  created_at: OffsetDateTime,
  deleted_at: Option[OffsetDateTime]
)

val users = Table.of[User]("users")
  .withPrimary("id")
  .withDefault("id")
  .withDefault("created_at")
  .withUnique("email")

// id and created_at are defaulted — omit them
val insert = users
  .insert((email = "alice@example.com", age = 30, deleted_at = None))
  .compile
```

Missing a required column:

```scala mdoc:fail
// email has no default and is not provided
users.insert((age = 30, deleted_at = None))
```

Wrong type:

```scala mdoc:fail
users.insert((email = 42, age = 30, deleted_at = None))
```

## Batch INSERT

`.values(rows)` accepts any `cats.Reducible` — `NonEmptyList`, `NonEmptyVector`, etc.
At least one row is a **type-level** guarantee.

```scala mdoc:silent
import cats.data.NonEmptyList

val row1 = (email = "a@example.com", age = 25, deleted_at = None)
val row2 = (email = "b@example.com", age = 32, deleted_at = None)

val batch = users.insert.values(NonEmptyList.of(row1, row2)).compile
```

Vararg form is also available:

```scala mdoc:silent
val batch2 = users.insert.values(row1, row2).compile
```

## RETURNING

Append `RETURNING` to get back columns (or the whole row) as a `CompiledQuery[R]`:

```scala mdoc:silent
// Single column
val insertReturningId = users
  .insert((email = "bob@example.com", age = 28, deleted_at = None))
  .returning(u => u.id)
  .compile

// Whole row
val insertReturningAll = users
  .insert((email = "carol@example.com", age = 35, deleted_at = None))
  .returningAll
  .compile
```

With RETURNING you use query execution methods:

```scala mdoc:compile-only
val session: skunk.Session[cats.effect.IO] = null
insertReturningId.unique(session)  // IO[UUID]
insertReturningAll.unique(session) // IO[(id: UUID, email: String, ...)]
```

## ON CONFLICT

```scala mdoc:silent
val row = (email = "dave@example.com", age = 40, deleted_at = None)

// Do nothing on any conflict
val upsertNothing = users.insert(row).onConflictDoNothing.compile

// Do nothing on conflict with a specific column
val upsertNothing2 = users.insert(row)
  .onConflict(u => u.email)
  .doNothing
  .compile

// Update on conflict — use `excluded` to access the incoming row
val upsert = users.insert(row)
  .onConflict(u => u.email)
  .doUpdateFromExcluded((t, ex) => (t.age := ex.age, t.deleted_at := ex.deleted_at))
  .compile

// Update with a literal value
val upsert2 = users.insert(row)
  .onConflict(u => u.email)
  .doUpdate(u => u.age := 99)
  .compile
```
