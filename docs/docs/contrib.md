# Contrib modules

Postgres extensions packaged in-core under `skunk.sharp.contrib.*`. No extra Scala
dependencies — every module ships an opaque tag (where applicable), a skunk codec, a
`PgTypeFor` instance, and operator / function extensions.

Each module that defines a tag uses `PgTypeFor.instanceWithExtension(codec, "<ext>")`,
so the [schema validator](schema-validation.md) auto-discovers the dependency from any
column declared with that tag. Function-only modules (pgcrypto, fuzzystrmatch) expose
`<Module>.RequiredExtension`; pass it via `extraExtensions = Set(...)` when calling
`SchemaValidator.validate`.

| Module | Extension | Tag types | Operators / functions |
| --- | --- | --- | --- |
| `citext` | `citext` | `Citext` | `PgCitext.toCitext` |
| `ltree` | `ltree` | `LTree`, `LQuery`, `LTxtQuery` | `matches`, `matchesTxt`, `isAncestorOf`, `isDescendantOf`, `concat`; `PgLtree.lca / nlevel / subltree / subpath / index / text2ltree / ltree2text` |
| `hstore` | `hstore` | `Hstore` | `get`, `hasKey`, `contains`, `containedBy`, `deleteKey`; `PgHstore.hstoreToJson / defined` |
| `pg_trgm` | `pg_trgm` | — | `similarTrgm`, `trgmDistance`, `wordSimilar`, `wordTrgmDistance`, `strictWordSimilar`, `strictWordTrgmDistance`; `PgTrgm.similarity / wordSimilarity / strictWordSimilarity / showLimit` |
| `pgcrypto` | `pgcrypto` | — | `PgCrypto.crypt / genSalt / digest / hmac` |
| `fuzzystrmatch` | `fuzzystrmatch` | — | `PgFuzzy.levenshtein / soundex / metaphone / dmetaphone / dmetaphoneAlt` |
| `pgvector` | `vector` | `PgVector[N]` | `cosineDistance`, `l2Distance`, `negativeInnerProduct`, `l1Distance`; `PgVector.dims / norm / normalize / innerProduct / avg` |

All examples below assume:

```scala mdoc:silent
import skunk.sharp.dsl.*
import java.util.UUID
```

## citext

Case-insensitive text. `Citext <: String`, so all string operators work; declaring a
column as `Citext` (instead of bare `String`) makes `Table.of[T]` pick the `citext`
codec and signals the validator to require `CREATE EXTENSION citext`.

```scala mdoc:silent
import skunk.sharp.contrib.citext.*
import skunk.sharp.contrib.citext.Citext.given

case class Account(id: UUID, email: Citext)
val accounts = Table.of[Account]("accounts").withPrimary("id")

// Existing String operators apply directly — Citext flows as String.
val q = accounts.select
  .where(a => a.email === Param.bind(Citext("Alice@Example.COM")))
  .compile
```

Cast a regular `String` expression to `citext` with `PgCitext.toCitext` for an inline
case-insensitive comparison without changing the column type.

## ltree

Hierarchical label-tree (`top.science.astronomy`). Three opaque tags: `LTree`, `LQuery`
(pattern matcher), `LTxtQuery` (boolean text query). All `<: String`.

```scala mdoc:silent
import skunk.sharp.contrib.ltree.*
import skunk.sharp.contrib.ltree.LTree.given
import skunk.sharp.contrib.ltree.LQuery.given

case class Category(id: UUID, path: LTree)
val categories = Table.of[Category]("categories").withPrimary("id")

// path ~ 'top.*.astronomy{1,2}'
val pattern = categories.select
  .where(c => c.path.matches(Param.bind(LQuery("top.*.astronomy{1,2}"))))
  .compile

// Ancestor / descendant
val under = categories.select
  .where(c => c.path.isDescendantOf(Param.bind(LTree("top.science"))))
  .compile

// Functions: number of labels per path
val depths = categories.select(c => PgLtree.nlevel(c.path)).compile
```

## hstore

Flat key/value store with nullable text values. `Hstore <: Map[String, Option[String]]`.

```scala mdoc:silent
import skunk.sharp.contrib.hstore.*
import skunk.sharp.contrib.hstore.Hstore.given

case class Item(id: UUID, attrs: Hstore)
val items = Table.of[Item]("items").withPrimary("id")

// attrs ? 'color' — does the key exist?
val withColor = items.select
  .where(i => i.attrs.hasKey(lit("color")))
  .compile

// attrs @> '"size"=>"M"'::hstore — contains a given pair
val mediums = items.select
  .where(i => i.attrs.contains(Param.bind(Hstore("size" -> Some("M")))))
  .compile

// attrs -> 'color' — value (nullable)
val colors = items.select(i => i.attrs.get(lit("color"))).compile
```

## pg_trgm

Trigram similarity. Operators apply to any `String`-tagged expression (so `Citext`,
`Varchar[N]`, `LTree`, … all work via `Stripped[T] <:< String` evidence):

```scala mdoc:silent
import skunk.sharp.contrib.pgtrgm.*

case class Doc(id: UUID, title: String)
val docs = Table.of[Doc]("docs").withPrimary("id")

// title % 'searchterm' — above the current similarity threshold
val fuzzy = docs.select
  .where(d => d.title.similarTrgm(lit("seerchterm")))
  .compile

// ORDER BY title <-> 'searchterm' — closest match first
val byClosest = docs.select
  .where(d => d.title.similarTrgm(lit("seerch")))
  .orderBy(d => d.title.trgmDistance(lit("seerch")).asc)
  .compile

// Functions return Float in [0, 1]
val scored = docs
  .select(d => (d.title, PgTrgm.similarity(d.title, lit("seerch"))))
  .compile
```

Function-only contribs don't appear in column types, so the validator can't auto-detect
them. Opt in explicitly:

```scala mdoc:compile-only
import skunk.sharp.validation.*
import cats.effect.IO

val session: skunk.Session[IO] = null
SchemaValidator.validateOrRaise[IO](
  session,
  Seq(docs),
  extraExtensions = Set(PgTrgm.RequiredExtension)
)
```

## pgcrypto

Server-side crypto: password hashing (`crypt` + `gen_salt`), `digest`, `hmac`.
Function-only; pass `extraExtensions = Set(PgCrypto.RequiredExtension)` for validation.

```scala mdoc:silent
import skunk.sharp.contrib.pgcrypto.*

case class Login(id: UUID, email: String, password_hash: String)
val logins = Table.of[Login]("logins").withPrimary("id")

// SELECT 1 ... WHERE password_hash = crypt('plain', password_hash)
val authenticate = logins.select(_ => lit(1))
  .where(l => l.password_hash === PgCrypto.crypt(Param[String], l.password_hash))
  .compile

// Hash a new password on INSERT (use as RHS of a SET clause / VALUES projection)
val saltExpr = PgCrypto.genSalt(lit("bf"), lit(12))
val hashed   = PgCrypto.crypt(Param[String], saltExpr)
```

## fuzzystrmatch

Edit-distance and phonetic functions. Same validation opt-in pattern as pgcrypto /
pg_trgm.

```scala mdoc:silent
import skunk.sharp.contrib.fuzzystrmatch.*

case class Person(id: UUID, name: String)
val people = Table.of[Person]("people").withPrimary("id")

// levenshtein(name, 'jonathon') <= 2
val close = people.select
  .where(p => PgFuzzy.levenshtein(p.name, lit("jonathon")) <= lit(2))
  .compile

// Phonetic match
val sounds = people.select
  .where(p => PgFuzzy.soundex(p.name) === PgFuzzy.soundex(lit("Catherine")))
  .compile
```

## pgvector

[pgvector](https://github.com/pgvector/pgvector) stores embeddings — the fixed-length float vectors an embedding
model produces for a piece of text — and finds the nearest ones. It's the usual way to do semantic search and RAG
retrieval inside Postgres. `PgVector[N]` is a `vector(N)`: the dimension is part of the type, so a query vector
from a model with a different dimension doesn't compile against the column, and a value of the wrong length can't be
constructed.

```scala mdoc:silent
import skunk.sharp.contrib.pgvector.*

case class Chunk(id: Long, doc: String, content: String, embedding: PgVector[3])

val chunks = Table.of[Chunk]("chunks").withPrimary("id").withDefault("id")

val query = PgVector[3](0.9f, 0.1f, 0f)   // in practice: the embedding of the user's question
```

The canonical query is top-k by distance, usually filtered by ordinary columns. Select only the columns you need —
there's no point fetching the embeddings back — and pass the query vector as a named parameter:

```scala mdoc:silent
// SELECT "id", "content" FROM "chunks" WHERE "doc" = $1 ORDER BY "embedding" <=> $2 ASC LIMIT 5
val topK = chunks
  .select(c => (c.id, c.content))
  .where(c => c.doc === Param.named["doc", String])
  .orderBy(c => c.embedding.cosineDistance(Param.named["query", PgVector[3]]).asc)
  .limit(5)
  .compile
// topK.run(session)((doc = "handbook", query = query))
```

| Method | Operator | Use |
| --- | --- | --- |
| `a.cosineDistance(b)` | `<=>` | the usual choice for text embeddings (`vector_cosine_ops` index) |
| `a.l2Distance(b)` | `<->` | Euclidean (`vector_l2_ops`) |
| `a.negativeInnerProduct(b)` | `<#>` | for normalised vectors; ascending = most similar (`vector_ip_ops`) |
| `a.l1Distance(b)` | `<+>` | taxicab (pgvector 0.7+, `vector_l1_ops`) |

Each returns a `Double`, so it can also be selected or filtered on. Functions live on the companion:
`PgVector.dims`, `PgVector.norm`, `PgVector.normalize` (`l2_normalize`), `PgVector.innerProduct` and the
aggregate `PgVector.avg` (a centroid).

To load a batch of embeddings in one statement, pass the rows as one typed parameter with `Pg.unnestRows`
(`vector[]` arrays are supported, so embedding columns work there too):

```scala mdoc:silent
import skunk.sharp.dsl.given

case class NewChunk(doc: String, content: String, embedding: PgVector[3])

val load = chunks.insert.from(Pg.unnestRows[NewChunk]("rows").alias("r").select).compile
// load.run(session)((rows = batch))
```

Indexes (`CREATE INDEX … USING hnsw (embedding vector_cosine_ops)`) belong in migrations, like the rest of the
schema. The [schema validator](schema-validation.md) compares the declared dimension with the column's, so a
`PgVector[1536]` declared against a `vector(3072)` column is reported as a `TypeMismatch`.

Ordering the **whole row** (`chunks.select.orderBy(…)`) by a parameterised distance is a compile error for now — the
whole-row builder doesn't carry ORDER BY parameters yet ([#109](https://github.com/ragb/skunk-sharp/issues/109)) — so use a projection as above.

