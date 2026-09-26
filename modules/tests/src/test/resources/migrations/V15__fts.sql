-- Full-text search: a generated tsvector (title weighted above body) with a GIN index.
CREATE TABLE fts_docs (
  id    serial PRIMARY KEY,
  title text   NOT NULL,
  body  text   NOT NULL,
  tsv   tsvector GENERATED ALWAYS AS (
          setweight(to_tsvector('english', title), 'A') || setweight(to_tsvector('english', body), 'B')
        ) STORED
);

CREATE INDEX fts_docs_tsv_gin ON fts_docs USING gin (tsv);
