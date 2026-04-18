CREATE TABLE tags (
  id      uuid PRIMARY KEY,
  post_id uuid NOT NULL REFERENCES posts (id),
  name    text NOT NULL
);
