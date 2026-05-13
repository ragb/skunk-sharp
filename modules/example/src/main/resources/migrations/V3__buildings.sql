-- Round 3: introduce a Buildings concept tied to a real-world location.
-- Rooms now live inside Buildings; queries like "every room within N metres of (lat, lon)"
-- naturally join through the building's `geom`.
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE buildings (
  id      UUID                       PRIMARY KEY DEFAULT gen_random_uuid(),
  name    TEXT                       NOT NULL,
  address TEXT                       NOT NULL,
  -- WGS84 (SRID 4326) lat/lon point. Constrained to Point so the column always carries the
  -- right geometry shape — anything else (LineString, Polygon, …) is rejected at insert.
  geom    geometry(Point, 4326)      NOT NULL
);

-- The standard PostGIS GiST index — what makes ST_DWithin / ST_Intersects / && fast.
CREATE INDEX IF NOT EXISTS buildings_geom_gist
  ON buildings USING gist (geom);

-- Rooms hang off Buildings. ON DELETE CASCADE so removing a building takes its rooms
-- (and, via the existing cascade on bookings.room_id, the bookings) with it.
-- Migrations run on an empty DB in the test fixture, so we can add the FK NOT NULL
-- directly without a backfill step.
ALTER TABLE rooms
  ADD COLUMN building_id UUID NOT NULL REFERENCES buildings (id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS rooms_building_id_idx
  ON rooms (building_id);
