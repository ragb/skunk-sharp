CREATE EXTENSION IF NOT EXISTS vector;

-- Tiny 3-dimensional "embeddings" so the expected nearest neighbours are easy to reason about.
CREATE TABLE chunks (
  id        bigserial PRIMARY KEY,
  doc       text      NOT NULL,
  content   text      NOT NULL,
  embedding vector(3) NOT NULL
);

CREATE INDEX chunks_embedding_hnsw ON chunks USING hnsw (embedding vector_cosine_ops);
