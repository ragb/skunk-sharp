# Full-text search

Postgres's built-in full-text search — no extension needed. A `tsvector` is a document
parsed into normalised words (lexemes, with positions); a `tsquery` is a query over them;
`@@` asks whether one matches the other. Everything lives in `skunk.sharp.fts`:

```scala mdoc:silent
import skunk.sharp.dsl.*
import skunk.sharp.fts.*
```

## Storing the document vector

The usual setup keeps the `tsvector` in a generated column with a GIN index, defined in a
migration (we don't generate DDL):

```sql
CREATE TABLE docs (
  id    serial PRIMARY KEY,
  title text NOT NULL,
  body  text NOT NULL,
  tsv   tsvector GENERATED ALWAYS AS (
          setweight(to_tsvector('english', title), 'A') || setweight(to_tsvector('english', body), 'B')
        ) STORED
);
CREATE INDEX ON docs USING gin (tsv);
```

Declare the column as `TsVector` (it's nullable, like any generated column without
`NOT NULL`) and mark it `.withGenerated`, so inserts leave it to Postgres:

```scala mdoc:silent
case class Doc(id: Int, title: String, body: String, tsv: Option[TsVector])

val docs = Table.of[Doc]("docs").withPrimary("id").withDefault("id").withGenerated("tsv")
```

## Searching

`websearch_to_tsquery` understands search-box syntax — `"exact phrase"`, `-excluded`,
`or` — and never errors on odd input, so it's the right parser for what users type. A
ranked search with highlighted snippets, compiled once with a named parameter:

```scala mdoc:silent
val query = Fts.websearchToTsQuery("english", Param.named["q", String])

// SELECT "title", ts_rank("tsv", …), ts_headline('english'::regconfig, "body", …)
// FROM "docs" WHERE "tsv" @@ websearch_to_tsquery('english'::regconfig, $3) ORDER BY ts_rank(…) DESC LIMIT 10
val search = docs
  .select(d => (d.title, Fts.tsRank(d.tsv, query), Fts.tsHeadline("english", d.body, query)))
  .where(d => d.tsv.matches(query))
  .orderBy(d => Fts.tsRank(d.tsv, query).desc)
  .limit(10)
  .compile
// search.run(session)((q = "fat rats -cat"))
```

The `setweight` labels in the generated column make title matches (`A`) rank above body
matches (`B`).

## Reference

| Scala | SQL | |
| --- | --- | --- |
| `doc.matches(q)` | `doc @@ q` | the search predicate (GIN-indexable) |
| `a.concat(b)` | `(a \|\| b)` | concatenate documents |
| `q1.andQuery(q2)`, `q1.orQuery(q2)`, `q.negate`, `q1.followedBy(q2)` | `&&`, `\|\|`, `!!`, `<->` | combine queries |
| `Fts.toTsVector(doc)` / `Fts.toTsVector(config, doc)` | `to_tsvector` | parse a document |
| `Fts.setWeight(doc, "A")` | `setweight` | weight `A`–`D`, for ranking |
| `Fts.toTsQuery` | `to_tsquery` | operator syntax (`fat & (rat \| cat)`); errors on bad input |
| `Fts.plainToTsQuery` | `plainto_tsquery` | all words ANDed |
| `Fts.phraseToTsQuery` | `phraseto_tsquery` | the words as a phrase |
| `Fts.websearchToTsQuery` | `websearch_to_tsquery` | search-box syntax; never errors |
| `Fts.tsRank`, `Fts.tsRankCd` | `ts_rank`, `ts_rank_cd` | relevance (`Float`); `_cd` also rewards proximity |
| `Fts.tsHeadline` | `ts_headline` | the text with matches marked, for snippets |

Every function taking a `config` accepts a literal or a `Param[String]` — it's cast to
`regconfig`. Query combinators render parenthesised: `@@`, `&&`, `||`, `!!` and `<->` share
one precedence level in Postgres, so `doc @@ q1 && q2` would otherwise parse as
`(doc @@ q1) && q2`. (They're named `andQuery` / `orQuery` because `and` / `or` are the
boolean `Where` combinators.)

`TsVector("…")` / `TsQuery("…")` wrap text as-is (unchecked, like other type tags); in
practice values come from the functions above.
