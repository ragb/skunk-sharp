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
