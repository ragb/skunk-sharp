CREATE TABLE app.products (
  id    uuid         PRIMARY KEY,
  name  varchar(256) NOT NULL,
  price numeric(10, 2) NOT NULL
);
