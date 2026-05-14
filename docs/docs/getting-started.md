# Getting Started

## Installation

skunk-sharp is published to **GitHub Packages**, so add the resolver alongside the
dependencies:

```scala
// build.sbt
resolvers += "skunk-sharp @ GitHub Packages" at
  "https://maven.pkg.github.com/ragb/skunk-sharp"

libraryDependencies += "io.github.ragb" %% "skunk-sharp-core" % "@VERSION@"

// Optional modules
libraryDependencies += "io.github.ragb" %% "skunk-sharp-iron"    % "@VERSION@"
libraryDependencies += "io.github.ragb" %% "skunk-sharp-refined" % "@VERSION@"
libraryDependencies += "io.github.ragb" %% "skunk-sharp-circe"   % "@VERSION@"
libraryDependencies += "io.github.ragb" %% "skunk-sharp-postgis" % "@VERSION@"
```

GitHub Packages requires authentication **even for public packages**. Add a GitHub
[personal access token](https://github.com/settings/tokens) with the `read:packages`
scope to your sbt credentials — e.g. in `~/.sbt/1.0/github.sbt`:

```scala
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "YOUR_GITHUB_USERNAME",
  sys.env("GITHUB_TOKEN")
)
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

Static-by-default: literal values go through `lit(v)` (compile-time constant, inlined as `'v'`)
or `Param.bind(v)` (bake the value into a Void-args fragment). Use `Param[T]` for deferred
parameters that get supplied at execute time.

```scala mdoc:silent
// SELECT — compiles to a CompiledQuery[...]
val allAdults = users.select
  .where(u => u.age >= lit(18))
  .orderBy(u => u.created_at.desc)
  .compile

// INSERT — defaulted columns (id, created_at) can be omitted
val insertUser = users
  .insert((email = "alice@example.com", age = 30, deleted_at = None))
  .compile

// UPDATE
val updateEmail = users.update
  .set(u => u.email := lit("new@example.com"))
  .where(u => u.id === Param.bind(UUID.randomUUID()))
  .compile

// DELETE
val deleteInactive = users.delete
  .where(u => u.deleted_at.isNotNull)
  .compile
```

All four calls above are **pure** — they build an `AppliedFragment` at compile time, bind
the user-supplied values, and return a `CompiledQuery[Args, R]` or `CompiledCommand[Args]`.
Nothing touches the network until you call an execution method with a session:

```scala mdoc:compile-only
val session: skunk.Session[cats.effect.IO] = null
allAdults.stream(session)    // fs2.Stream[IO, (id: UUID, email: String, ...)]
insertUser.run(session)      // IO[skunk.data.Completion]
updateEmail.run(session)     // IO[skunk.data.Completion]
deleteInactive.run(session)  // IO[skunk.data.Completion]
```

## Type safety in action

The compiler catches misuse before it reaches the database:

```scala mdoc:fail
// Wrong type — Int column compared to String
users.select.where(u => u.age === lit("not a number"))
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
