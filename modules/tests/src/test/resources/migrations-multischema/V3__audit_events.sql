CREATE TABLE audit.events (
  id         bigserial    PRIMARY KEY,
  action     varchar(64)  NOT NULL,
  product_id uuid         NULL REFERENCES app.products (id),
  occurred_at timestamptz NOT NULL DEFAULT now()
);
