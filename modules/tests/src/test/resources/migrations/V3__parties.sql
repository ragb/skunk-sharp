CREATE TABLE parties (
  id          bigserial       PRIMARY KEY,
  name        varchar(64)     NOT NULL,
  postal_code char(5)         NOT NULL,
  balance     numeric(10, 2)  NOT NULL DEFAULT 0
);
