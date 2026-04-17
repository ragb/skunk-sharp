# skunk-sharp

A Scala 3 library for **compile-time checked Postgres queries** on top of [skunk](https://typelevel.org/skunk). Describe a table once, then write SELECT / INSERT / UPDATE / DELETE statements where column names, operator/value types, and nullability are verified by the compiler. Validate table descriptions against a live database at service init.

> :warning: This project is in early development. APIs will change.

## Modules

- `skunk-sharp-core` — the DSL, `Table` descriptions, WHERE/SELECT/INSERT/UPDATE/DELETE builders, and the schema validator.
- `skunk-sharp-iron` — optional [Iron](https://iltotore.github.io/iron/) refinement support (`String :| MaxLength[N]` → `VARCHAR(N)`, etc).

## License

Apache-2.0. See [LICENSE](LICENSE).
