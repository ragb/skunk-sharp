-- btree_gist enables btree-style operators (e.g. `=` on UUID) inside GiST indexes — required for the
-- `EXCLUDE USING gist (room_id WITH =, period WITH &&)` constraint below. The fresh
-- postgres:18-alpine image used by the test container does not have it loaded by default.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE rooms (
  id       UUID      PRIMARY KEY DEFAULT gen_random_uuid(),
  name     TEXT      NOT NULL,
  capacity INT       NOT NULL CHECK (capacity > 0)
);

CREATE TABLE bookings (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id      UUID        NOT NULL REFERENCES rooms (id) ON DELETE CASCADE,
  booker_name  TEXT        NOT NULL,
  title        TEXT        NOT NULL,
  period       DATERANGE   NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT no_overlap EXCLUDE USING gist (room_id WITH =, period WITH &&)
);
