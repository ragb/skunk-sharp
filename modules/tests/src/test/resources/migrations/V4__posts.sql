CREATE TABLE posts (
  id         uuid         PRIMARY KEY,
  user_id    uuid         NOT NULL REFERENCES users (id),
  title      text         NOT NULL,
  created_at timestamptz  NOT NULL DEFAULT now()
);
