-- Target / source pair for MERGE tests. `qty_x2` is generated (read-only).
CREATE TABLE merge_stock (
  sku    text PRIMARY KEY,
  qty    int  NOT NULL,
  note   text,
  qty_x2 int  GENERATED ALWAYS AS (qty * 2) STORED
);

CREATE TABLE merge_incoming (
  sku text PRIMARY KEY,
  qty int  NOT NULL
);
