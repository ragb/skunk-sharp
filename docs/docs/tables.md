# Tables and Views

## Defining a table

### From a case class

`Table.of[T]("table_name")` derives columns from the case class fields via `Mirror`.
Each field type must have a `PgTypeFor[T]` instance that maps it to a Postgres type.

```scala mdoc:silent
import skunk.sharp.dsl.*
import java.util.UUID
import java.time.OffsetDateTime

case class Post(
  id:         UUID,
  author_id:  UUID,
  title:      String,
  body:       String,
  published:  Boolean,
  created_at: OffsetDateTime
)

val posts = Table.of[Post]("posts")
  .withPrimary("id")
  .withDefault("id")
  .withDefault("created_at")
```

`.withPrimary`, `.withDefault`, and `.withUnique` all verify the column name exists **at
compile time**:

```scala mdoc:fail
// "nope" is not a column of Post
Table.of[Post]("posts").withPrimary("nope")
```

### With the column-by-column builder

When there is no case class, or when you want the row type to be a named tuple, use the
builder. The row type is `(id: UUID, email: String, age: Int, ...)` by default.

```scala mdoc:silent
val users = Table.builder("users")
  .column[UUID]("id")
  .column[String]("email")
  .column[Int]("age")
  .columnDefaulted[OffsetDateTime]("created_at")
  .columnOpt[OffsetDateTime]("deleted_at")
  .build
  .withPrimary("id")
  .withDefault("id")
  .withUnique("email")
```

`.columnDefaulted` marks a column as having a Postgres default (sequence, `DEFAULT now()`, …)
so INSERT can omit it. `.columnOpt[T]` declares a nullable column (`Option[T]` in Scala).

## Type tags — unambiguous codec selection

A bare `String` field maps to Postgres `text`. When the column is `varchar(256)`, use
`Varchar[256]` from `skunk.sharp.pg.tags`:

```scala mdoc:silent
import skunk.sharp.pg.tags.*

case class Account(
  id:       UUID,
  username: Varchar[64],
  bio:      Text,
  score:    Numeric[10, 2]
)

val accounts = Table.of[Account]("accounts")
  .withPrimary("id")
  .withDefault("id")
```

Available tags: `Text`, `Varchar[N]`, `Bpchar[N]`, `Int2`, `Int4`, `Int8`,
`Float4`, `Float8`, `Numeric[P, S]`, `Timestamptz`, `Date`, `Bytea`, `Jsonb`, `Json`,
plus range tags: `PgRange[A]`.

Tags are **opaque subtypes of their base type** (`Varchar[N] <: String`), so values flow
as plain `String` / `Int` at runtime with no boxing.

### Iron integration

With `skunk-sharp-iron`, Iron constraints route to the matching tag automatically:

```scala
import skunk.sharp.dsl.*
import skunk.sharp.iron.given
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.*
import io.github.iltotore.iron.constraint.numeric.*

case class Profile(
  id:       UUID,
  username: String :| MaxLength[64],   // → varchar(64)
  score:    Int    :| Positive          // → int4, enforced at the Scala level
)
```

## Views

`View.of[T]("view_name")` works exactly like `Table.of` but the resulting relation is
read-only. `view.select` compiles; `view.insert` / `.update` / `.delete` are compile errors.

```scala mdoc:silent
case class ActiveUser(id: UUID, email: String, age: Int)
val active_users = View.of[ActiveUser]("active_users")
```

```scala mdoc:fail
// Views are read-only
active_users.insert((id = UUID.randomUUID(), email = "a@b.com", age = 30))
```

Row locking (`.forUpdate`, `.forShare`, …) on a view is also a compile error — Postgres
would reject it at runtime anyway.

## Constraints

| Method | What it does |
| --- | --- |
| `.withPrimary("col")` | Marks the column as the primary key (compile-time name check) |
| `.withUnique("col")` | Marks the column as unique (used in `ON CONFLICT` clauses) |
| `.withDefault("col")` | Marks the column as having a Postgres default (INSERT may omit it) |

Multiple constraints can be chained: `.withPrimary("id").withDefault("id").withUnique("email")`.
