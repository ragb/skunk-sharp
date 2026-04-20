CREATE TABLE array_posts (
  id    integer PRIMARY KEY,
  tags  text[]  NOT NULL,
  score integer NOT NULL
);
