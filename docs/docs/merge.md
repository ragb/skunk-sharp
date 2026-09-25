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

A branch after an **unconditional** branch of the same kind can never fire, and Postgres rejects it ("unreachable
WHEN clause"). The DSL rejects it at compile time: after `.whenMatched.update(…)`, another `.whenMatched…` doesn't
compile. Put conditional branches first and the catch-all last.

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
`.on`, a branch condition, or an action — become the compiled command's arguments, in SQL order. MERGE statements
tend to collect several same-typed parameters, which is where
[named parameters](select.md#named-parameters) pay off:

```scala mdoc:silent
val filtered = stock
  .merge(incoming.select.where(i => i.qty >= Param.named["minQty", Int]).alias("src"))
  .on(r => r.stock.sku === r.src.sku)
  .whenMatched.update(r => r.stock.qty := r.src.qty)
  .whenNotMatched(s => s.qty < Param.named["maxQty", Int]).insert(s => (sku = s.sku, qty = s.qty))
  .compile
// filtered.run(session)((minQty = 1, maxQty = 100))
```

A common pattern is a staging table with the target's shape: `stock.merge(stock.renamed("stock_staging"))`.

## Batches as typed parameters

To merge a batch of rows that lives in your application, don't build a `VALUES` list per call: that bakes the values
into the SQL, so every batch is a different statement. Pass the batch as **one** typed parameter with
`Pg.unnestRows[Row]`. `Row` is a case class or named tuple; at execute time the `List[Row]` is split into one Postgres
array per field (`unnest($1, $2)`), so a single prepared statement serves any batch size. The array codecs need
`import skunk.sharp.dsl.given`:

```scala mdoc:silent
import skunk.sharp.dsl.given

case class StockLine(sku: String, qty: Int)

val syncStock: CommandTemplate[List[StockLine]] = stock
  .merge(Pg.unnestRows[StockLine].alias("batch"))
  .on(r => r.stock.sku === r.batch.sku)
  .whenMatched.update(r => r.stock.qty := r.batch.qty)
  .whenNotMatched.insert(b => (sku = b.sku, qty = b.qty))
  .compile
// MERGE INTO "stock" USING unnest($1, $2) AS "batch"("sku", "qty") ON …
// syncStock.run(session)(List(StockLine("a", 1), StockLine("b", 2)))
```

Give the batch a name — `Pg.unnestRows[StockLine]("lines")` — to use it in a fully
named statement: `syncStock.run(session)((lines = batch, …))`.

If you already hold one list per column, `Pg.unnestAsRelation((sku = Param[List[String]], qty = Param[List[Int]]))`
takes them as separate parameters. The lists must be the same length (checked when the statement is encoded). The
example app's `PUT /api/v1/buildings/{id}/rooms` endpoint syncs a building's rooms this way in one statement.

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
