-- Exercise composite primary keys and named composite unique constraints.
CREATE TABLE orders (
  tenant_id  uuid        NOT NULL,
  order_id   bigint      NOT NULL,
  slug       text        NOT NULL,
  external_id text       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, order_id),
  CONSTRAINT uq_orders_tenant_slug UNIQUE (tenant_id, slug),
  CONSTRAINT uq_orders_external_id UNIQUE (external_id)
);
