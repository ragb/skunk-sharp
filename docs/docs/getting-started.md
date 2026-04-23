# Getting Started

## Installation

```scala
// build.sbt
libraryDependencies += "com.ruiandrebatista" %% "skunk-sharp-core" % "@VERSION@"

// Optional modules
libraryDependencies += "com.ruiandrebatista" %% "skunk-sharp-iron"  % "@VERSION@"
libraryDependencies += "com.ruiandrebatista" %% "skunk-sharp-circe" % "@VERSION@"
```

Requires **Scala 3.7+** (uses named tuples, stable since 3.7).

## A complete example

Define a table, build queries, execute against a skunk session.

```scala mdoc:silent
import skunk.sharp.dsl.*
import java.util.UUID
import java.time.OffsetDateTime

// 1. Describe the table once.
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
```

With the table in scope, the DSL entry points are extension methods directly on `users`:

```scala mdoc:silent
// SELECT — compiles to a CompiledQuery[...]
val allAdults = users.select
  .where(u => u.age >= 18)
  .orderBy(u => u.created_at.desc)
  .compile

// INSERT — defaulted columns (id) can be omitted
val insertUser = users
  .insert((email = "alice@example.com", age = 30, deleted_at = None))
  .compile

// UPDATE
val updateEmail = users.update
  .set(u => u.email := "new@example.com")
  .where(u => u.id === UUID.randomUUID())
  .compile

// DELETE
val deleteInactive = users.delete
  .where(u => u.deleted_at.isNotNull)
  .compile
```

All four calls above are **pure** — they build an `AppliedFragment` at compile time, bind
the user-supplied values, and return a `CompiledQuery[R]` or `CompiledCommand`. Nothing
touches the network until you call an execution method with a session:

```scala
// session: skunk.Session[IO]
allAdults.stream(session)    // fs2.Stream[IO, (id: UUID, email: String, ...)]
insertUser.run(session)      // IO[skunk.data.Completion]
updateEmail.run(session)     // IO[skunk.data.Completion]
deleteInactive.run(session)  // IO[skunk.data.Completion]
```

## Type safety in action

The compiler catches misuse before it reaches the database:

```scala mdoc:fail
// Wrong type — Int column compared to String
users.select.where(u => u.age === "not a number")
```

```scala mdoc:fail
// isNull on a non-nullable column
users.select.where(u => u.age.isNull)
```

```scala mdoc:fail
// INSERT missing a required column (email has no default)
users.insert((age = 30, deleted_at = None))
```

```scala mdoc:fail
// DELETE without a WHERE — use .deleteAll to confirm
users.delete.compile
```
