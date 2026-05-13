CREATE TABLE accounts (
  id    integer PRIMARY KEY,
  email citext  NOT NULL,
  body  text    NOT NULL
);

CREATE TABLE folders (
  id   integer PRIMARY KEY,
  path ltree   NOT NULL
);

CREATE TABLE things (
  id    integer PRIMARY KEY,
  props hstore  NOT NULL
);
