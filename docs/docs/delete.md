# DELETE

DELETE uses the same **staged builder** pattern as UPDATE:

```
users.delete  →  .where(…) or .deleteAll  →  .compile
```

Calling `.compile` without either `.where` or `.deleteAll` is a compile error.

## Basic DELETE

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

val deleteOne = users.delete
  .where(u => u.id === UUID.randomUUID())
  .compile

val deleteInactive = users.delete
  .where(u => u.deleted_at.isNotNull)
  .compile
```

Forgetting `.where` / `.deleteAll`:

```scala mdoc:fail
users.delete.compile
```

## Bulk DELETE

```scala mdoc:silent
val deleteAll = users.delete.deleteAll.compile
```

## RETURNING

```scala mdoc:silent
case class Post(id: UUID, author_id: UUID, title: String, views: Int)
val posts = Table.of[Post]("posts").withPrimary("id").withDefault("id")

// Return the deleted rows
val deleteReturning = posts.delete
  .where(p => p.views < 10)
  .returningAll
  .compile
```

```scala
deleteReturning.stream(session)     // fs2.Stream[IO, (id: UUID, author_id: UUID, ...)]
deleteReturning.run(session)        // IO[List[(id: UUID, ...)]]
```

`.returning(p => p.id)` returns only specific columns; `.returningAll` returns the whole row.
