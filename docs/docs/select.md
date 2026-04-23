# SELECT

## Basic usage

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
```

```scala mdoc:silent
// Whole row — result type is a named tuple matching the case class fields
val q1 = users.select.compile

// With WHERE
val q2 = users.select
  .where(u => u.age >= 18 && u.email.like("%@example.com"))
  .compile

// ORDER BY, LIMIT, OFFSET
val q3 = users.select
  .where(u => u.deleted_at.isNull)
  .orderBy(u => u.created_at.desc)
  .limit(50)
  .offset(100)
  .compile
```

Execution methods on `CompiledQuery[R]`:

```scala mdoc:compile-only
val session: skunk.Session[cats.effect.IO] = null
q1.run(session)        // IO[List[R]]
q2.unique(session)     // IO[R]           — raises if 0 or >1 rows
q3.option(session)     // IO[Option[R]]   — raises if >1 row
q3.stream(session)     // fs2.Stream[IO, R]
```

## Projections

Select a subset of columns by passing a function to `.select(…)`:

```scala mdoc:silent
// Single column → CompiledQuery[String]
val emails = users.select(u => u.email).compile

// Multiple columns → CompiledQuery[(email: String, age: Int)]
val snap = users.select(u => (u.email, u.age)).compile
```

Map a multi-column projection to a case class with `.to[T]`:

```scala mdoc:silent
case class UserSnap(email: String, age: Int)
val snaps = users.select(u => (u.email, u.age)).to[UserSnap].compile
```

The `.to[T]` alignment is checked at compile time — field types must match the
projection tuple in order.

## DISTINCT

```scala mdoc:silent
val distinctAges = users.select(u => u.age).distinctRows.compile
```

## ORDER BY

`.orderBy` accepts one or more expressions:

```scala mdoc:silent
// Descending, with nulls sorted last
val ordered = users.select
  .orderBy(u => u.created_at.desc.nullsLast)
  .compile

// Multiple columns
val multi = users.select
  .orderBy(u => (u.age.asc, u.email.asc))
  .compile
```

## GROUP BY and aggregates

Aggregates live on `Pg`:

```scala mdoc:silent
case class Post(id: UUID, author_id: UUID, title: String, views: Int)
val posts = Table.of[Post]("posts").withPrimary("id").withDefault("id")

// Count posts per author
val perAuthor = posts
  .select(p => (p.author_id, Pg.count(p.id).as("n")))
  .groupBy(p => p.author_id)
  .compile

// HAVING
val popular = posts
  .select(p => (p.author_id, Pg.sum(p.views).as("total_views")))
  .groupBy(p => p.author_id)
  .having(p => Pg.sum(p.views) > 1000)
  .compile
```

Available aggregates: `Pg.countAll`, `Pg.count`, `Pg.countDistinct`, `Pg.sum`,
`Pg.avg`, `Pg.min`, `Pg.max`, `Pg.stringAgg`, `Pg.boolAnd`, `Pg.boolOr`.

## JOINs

JOINs produce a two-source (or N-source) builder. Column access uses the table name
as the prefix:

```scala mdoc:silent
// INNER JOIN
val joined = users
  .innerJoin(posts).on(r => r.users.id ==== r.posts.author_id)
  .select(r => (r.users.email, r.posts.title))
  .compile

// LEFT JOIN — right-side columns become nullable at the type level
val leftJoined = users
  .leftJoin(posts).on(r => r.users.id ==== r.posts.author_id)
  .select(r => (r.users.email, Pg.count(r.posts.id).as("n")))
  .groupBy(r => r.users.email)
  .compile

// CROSS JOIN — no .on(…) required
val crossed = users.crossJoin(posts)
  .select(r => (r.users.email, r.posts.title))
  .compile
```

Use `.alias("x")` when the same table appears more than once:

```scala mdoc:silent
val selfJoin = users.alias("u1")
  .innerJoin(users.alias("u2")).on(r => r.u1.age ==== r.u2.age)
  .select(r => (r.u1.email, r.u2.email))
  .compile
```

## Subqueries

Subqueries are written inline as builder expressions — no inner `.compile` needed.
Correlation (referencing an outer column) is automatic through lexical scope.

```scala mdoc:silent
// Scalar subquery in projection
val withCount = users.alias("u").select(u =>
  (u.email, posts.select(_ => Pg.countAll).where(p => p.author_id ==== u.id).asExpr)
).compile

// IN subquery
val withPosts = users.select
  .where(u => u.id.in(posts.select(p => p.author_id)))
  .compile

// EXISTS
val active = users.alias("u").select
  .where(u => Pg.exists(posts.select(_ => Pg.countAll).where(p => p.author_id ==== u.id)))
  .compile
```

## Row locking (tables only)

```scala mdoc:silent
val locked = users.select
  .where(u => u.id === UUID.randomUUID())
  .forUpdate
  .skipLocked
  .compile
```

`.forUpdate`, `.forShare`, `.forNoKeyUpdate`, `.forKeyShare` are available on `Table`
select builders only — calling them on a `View` is a compile error.

## FROM-less queries

Use the built-in `empty` relation for queries with no FROM clause:

```scala mdoc:silent
// SELECT now()
val now = empty.select(_ => Pg.now).compile
```
