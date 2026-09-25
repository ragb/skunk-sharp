# MERGE

`MERGE` (Postgres 15+) joins a target table with a source and runs a list of `WHEN` branches in order: the first
branch whose condition holds acts on the row. It covers what `INSERT … ON CONFLICT` can't: conditional deletes,
several branches with different actions, and (Postgres 17+) removing target rows that are missing from the source.

```text
target.merge(source)  →  .on(…)  →  one or more .when…(…).<action>  →  .compile / .returning(…)
```

`.compile` only exists once there is at least one `WHEN` branch.

## Upsert

```scala mdoc:silent
import skunk.sharp.dsl.*

case class Stock(sku: String, qty: Int, note: Option[String])
case class Incoming(sku: String, qty: Int)

val stock    = Table.of[Stock]("stock").withPrimary("sku").withDefault("note")
val incoming = Table.of[Incoming]("incoming")

// MERGE INTO "stock" USING "incoming" ON "stock"."sku" = "incoming"."sku"
//   WHEN MATCHED THEN UPDATE SET "qty" = "incoming"."qty"
//   WHEN NOT MATCHED THEN INSERT ("sku", "qty") VALUES ("incoming"."sku", "incoming"."qty")
val upsert = stock
  .merge(incoming)
  .on(r => r.stock.sku === r.incoming.sku)
  .whenMatched.update(r => r.stock.qty := r.incoming.qty)
  .whenNotMatched.insert(s => (sku = s.sku, qty = s.qty))
  .compile
```

Like a JOIN, the `.on` and `whenMatched` lambdas see both relations by name (or alias): `r.stock`, `r.incoming`.

## Branches

| Branch | The lambda sees | Actions |
| --- | --- | --- |
| `.whenMatched` / `.whenMatched(r => cond)` | target and source | `.update(…)`, `.delete`, `.doNothing` |
| `.whenNotMatched` / `.whenNotMatched(s => cond)` | the source row only | `.insert(…)`, `.doNothing` |
| `.whenNotMatchedBySource` / `.whenNotMatchedBySource(t => cond)` (PG 17+) | the target row only | `.update(…)`, `.delete`, `.doNothing` |

The views mirror what Postgres allows: a `WHEN NOT MATCHED` row has no target row, so the lambda simply has no
target columns to refer to.

```scala mdoc:silent
// Sync `stock` to `incoming`: update, delete zeroed rows, insert new ones, delete rows gone from the source.
val sync = stock
  .merge(incoming)
  .on(r => r.stock.sku === r.incoming.sku)
  .whenMatched(r => r.incoming.qty === 0).delete
  .whenMatched.update(r => r.stock.qty := r.incoming.qty)
  .whenNotMatched.insert(s => (sku = s.sku, qty = s.qty))
  .whenNotMatchedBySource.delete
  .compile
```

`.insert` gets the same compile-time checks as `table.insert`: every name must be a target column, every required
column must be present, generated columns (`.withGenerated`) can't be written, and each expression's type must fit
its column. `.update` uses the same `:=` assignments as UPDATE, including the ban on assigning generated columns; in a
`whenMatched` branch the source columns are readable but not assignable (`r.incoming.qty := …` doesn't compile).

```scala mdoc:fail
// Does not compile: insert is missing required column "qty".
stock.merge(incoming).on(r => r.stock.sku === r.incoming.sku)
  .whenNotMatched.insert(s => (sku = s.sku))
```

## Sources and parameters

The source can be any table, view, aliased relation, or aliased subquery. `Param`s anywhere — in a subquery source,
`.on`, a branch condition, or an action — become the compiled command's arguments, in SQL order:

```scala mdoc:silent
val filtered: CommandTemplate[(Int, Int)] = stock
  .merge(incoming.select.where(i => i.qty >= Param[Int]).alias("src"))
  .on(r => r.stock.sku === r.src.sku)
  .whenMatched.update(r => r.stock.qty := r.src.qty)
  .whenNotMatched(s => s.qty < Param[Int]).insert(s => (sku = s.sku, qty = s.qty))
  .compile
// filtered.run(session)((1, 100))
```

A common pattern is a staging table with the target's shape: `stock.merge(stock.renamed("stock_staging"))`.

## Batches as typed parameters

To merge a batch of rows that lives in your application, don't build a `VALUES` list per call: that bakes the values
into the SQL, so every batch is a different statement. Pass one array per column instead, through
`Pg.unnestAsRelation`, and the whole batch becomes typed arguments of **one** statement you compile once:

`List[T]` parameters map to Postgres arrays through the collection codecs, which need `import skunk.sharp.dsl.given`:

```scala mdoc:silent
import skunk.sharp.dsl.given

val syncStock: CommandTemplate[(List[String], List[Int])] = stock
  .merge(Pg.unnestAsRelation((sku = Param[List[String]], qty = Param[List[Int]])).alias("batch"))
  .on(r => r.stock.sku === r.batch.sku)
  .whenMatched.update(r => r.stock.qty := r.batch.qty)
  .whenNotMatched.insert(b => (sku = b.sku, qty = b.qty))
  .compile
// MERGE INTO "stock" USING unnest($1, $2) AS "batch"("sku", "qty") ON …
// syncStock.run(session)((List("a", "b"), List(1, 2)))
```

The arrays should have the same length — Postgres pads shorter ones with NULL. The example app's
`PUT /api/v1/buildings/{id}/rooms` endpoint uses this shape to sync a building's rooms in one statement.

## RETURNING (PG 17+)

`.returning` / `.returningTuple` can use columns from both sides, plus `Pg.mergeAction`, which is `'INSERT'`,
`'UPDATE'` or `'DELETE'` for each returned row:

```scala mdoc:silent
val audited = stock
  .merge(incoming)
  .on(r => r.stock.sku === r.incoming.sku)
  .whenMatched.update(r => r.stock.qty := r.incoming.qty)
  .whenNotMatched.insert(s => (sku = s.sku, qty = s.qty))
  .returningTuple(r => (Pg.mergeAction, r.stock.sku))
// audited.run(session): List[(String, String)]
```
