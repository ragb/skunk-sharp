CREATE TABLE users (
  id         uuid        PRIMARY KEY,
  email      text        NOT NULL UNIQUE,
  age        integer     NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);

CREATE VIEW active_users AS
  SELECT id, email, age
  FROM users
  WHERE deleted_at IS NULL;
