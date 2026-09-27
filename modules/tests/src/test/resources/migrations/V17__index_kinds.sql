-- Index kinds beyond plain btree, for the schema validator's index check (#132).
CREATE TABLE catalog (
  id         bigint PRIMARY KEY,
  sku        text      NOT NULL,
  name       text      NOT NULL,
  price      numeric   NOT NULL,
  tags       int[]     NOT NULL,
  available  daterange NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX catalog_tags_gin          ON catalog USING gin (tags);
CREATE INDEX catalog_available_gist    ON catalog USING gist (available);
CREATE INDEX catalog_created_brin      ON catalog USING brin (created_at);
CREATE INDEX catalog_name_pattern_idx  ON catalog (name text_pattern_ops);
CREATE INDEX catalog_name_c_idx        ON catalog (name COLLATE "C" DESC);
CREATE INDEX catalog_lower_name_idx    ON catalog (lower(name));
CREATE INDEX catalog_price_cover_idx   ON catalog (price) INCLUDE (name) WITH (fillfactor = 70);
CREATE UNIQUE INDEX catalog_sku_uidx   ON catalog (sku);
