-- Inbox of users pending promotion to the main `users` table; same shape so
-- `INSERT INTO users SELECT … FROM users_inbox` is the canonical test case.
CREATE TABLE users_inbox (
  id         uuid        PRIMARY KEY,
  email      text        NOT NULL UNIQUE,
  age        integer     NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);
