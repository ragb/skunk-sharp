# UPDATE

UPDATE uses a **staged builder** to prevent accidental bulk updates. The stages are:

```text
users.update  →  .set(…)  →  .where(…) or .updateAll  →  .compile
```

Calling `.compile` without either `.where` or `.updateAll` is a **compile error** — the
`.compile` method simply does not exist on the intermediate `UpdateWithSet` type.

## Basic UPDATE

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
  .withUnique("email")

// Single column
val updateEmail = users.update
  .set(u => u.email := "new@example.com")
  .where(u => u.id === UUID.randomUUID())
  .compile

// Multiple columns
val updateBoth = users.update
  .set(u => (u.email := "x@example.com", u.age := 31))
  .where(u => u.age < 18)
  .compile
```

## Bulk UPDATE

Use `.updateAll` to confirm you intentionally want no WHERE clause:

```scala mdoc:silent
val activateAll = users.update
  .set(u => u.deleted_at := None)
  .updateAll
  .compile
```

Missing `.where` / `.updateAll`:

```scala mdoc:fail
users.update.set(u => u.age := 0).compile
```

## RETURNING

```scala mdoc:silent
val updateReturning = users.update
  .set(u => u.age := 30)
  .where(u => u.id === UUID.randomUUID())
  .returningAll
  .compile

// Map back to the case class
val updateReturningUser = users.update
  .set(u => u.age := 30)
  .where(u => u.id === UUID.randomUUID())
  .returningAll
  .to[User]
  .compile
```

```scala mdoc:compile-only
val session: skunk.Session[cats.effect.IO] = null
updateReturning.option(session)      // IO[Option[(id: UUID, email: String, ...)]]
updateReturningUser.option(session)  // IO[Option[User]]
```

## Updating from a case class (patch pattern)

`.patch(p)` reads all fields of `p` and generates a `SET` clause. Useful with a
dedicated `Patch` case class where every field is `Option[_]`:

```scala mdoc:silent
case class UserPatch(email: Option[String], age: Option[Int])

val patch = users.update
  .patch(UserPatch(email = Some("patched@example.com"), age = None))
  .where(u => u.id === UUID.randomUUID())
  .compile
```

The field names in the patch type must all exist as columns in the table — verified at compile time.
