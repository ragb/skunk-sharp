-- Round 2: pull in a few Postgres contrib extensions so the example can show off the
-- skunk-sharp.contrib.* modules (citext, ltree, hstore, pg_trgm).
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Booker names are case-insensitive in practice ("alice" == "ALICE"). citext bakes that in
-- at the storage layer instead of forcing every WHERE clause through lower(...).
ALTER TABLE bookings
  ALTER COLUMN booker_name TYPE citext;

-- A trigram GIN index makes the `%`/`<->` operators on booker_name index-backed — what makes
-- fuzzy/autocomplete search fast in production. For a citext column we ask for the
-- gin_trgm_ops opclass against the underlying text representation.
CREATE INDEX IF NOT EXISTS bookings_booker_name_trgm
  ON bookings USING gin (booker_name gin_trgm_ops);

-- Every room lives somewhere — building.floor.wing — and operations want filters like "every
-- room on Dublin floor 3". ltree models exactly this; defaulting to 'unsorted' keeps any
-- existing rows valid.
ALTER TABLE rooms
  ADD COLUMN location ltree NOT NULL DEFAULT 'unsorted'::ltree;

-- Free-form amenity metadata: {projector => "4k", whiteboard => "true", videoconf => NULL}.
-- The natural shape for "we don't know what the keys will be ahead of time" — hstore is
-- denser than jsonb for flat key/value lookups and supports the @>/?  operators directly.
ALTER TABLE rooms
  ADD COLUMN amenities hstore NOT NULL DEFAULT ''::hstore;

-- GiST index on the path so `<@`/`@>` (descendant / ancestor) and label-pattern matching
-- (`~`) are index-backed.
CREATE INDEX IF NOT EXISTS rooms_location_gist
  ON rooms USING gist (location);
