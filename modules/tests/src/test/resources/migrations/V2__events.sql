CREATE TABLE events (
  id         bigserial    PRIMARY KEY,
  kind       text         NOT NULL,
  payload    text         NOT NULL,
  created_at timestamptz  NOT NULL DEFAULT now()
);
