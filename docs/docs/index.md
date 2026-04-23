# skunk-sharp

A Scala 3 library for **compile-time checked Postgres queries** on top of [skunk](https://typelevel.org/skunk).

Describe a table once. Write SELECT / INSERT / UPDATE / DELETE where column names, types, nullability, INSERT completeness, and mutability (table vs view) are all verified by the compiler. Optionally validate the declared table shape against a live database at service startup.

## What it is

```text
+---------+    describes     +-------+    compiles to     +--------------------+
|  Table  | --------------> |  DSL  | -----------------> | skunk.Fragment / Q |
|  View   |                 | query |                     | executed by skunk  |
+---------+                 +-------+                     +--------------------+
```

- **Column names are compile-time strings.** Typos are compile errors, not 404s at runtime.
- **Types match.** `u.age === "oops"` doesn't compile; `u.deleted_at.isNull` requires a nullable column.
- **INSERT completeness.** Omitting a required column is a compile error; defaulted columns can be omitted.
- **Mutability.** `view.insert(row)` is a compile error. Row locking on a view is a compile error.
- **Schema validation.** `SchemaValidator.validateOrRaise` diffs declared columns against `information_schema` at boot — catches drift before the first query runs.

## What it is not

skunk-sharp does **not** replace SQL. The DSL mirrors the SQL you would otherwise write by hand. Use `sql"…"` directly whenever the DSL doesn't fit — complex CTEs, procedural code, niche operators. Mixed use is expected and fine; everything shares the same skunk session.

## Modules

| Artifact | Description |
| --- | --- |
| `skunk-sharp-core` | The DSL — tables, views, SELECT/INSERT/UPDATE/DELETE, schema validation |
| `skunk-sharp-iron` | Optional [Iron](https://iltotore.github.io/iron/) refinement support |
| `skunk-sharp-circe` | Postgres `json`/`jsonb` via [skunk-circe](https://typelevel.org/skunk) |

## Status

Early development. APIs will change. Scala 3.8+ only.

@:navigationTree { entries = [ { target = "/" } ] }
