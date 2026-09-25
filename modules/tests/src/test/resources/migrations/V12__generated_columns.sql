-- One STORED and one VIRTUAL (PG 18) generated column, for `.withGenerated`.
CREATE TABLE priced_items (
  id          serial  PRIMARY KEY,
  name        text    NOT NULL,
  price_net   numeric NOT NULL,
  vat_rate    numeric NOT NULL,
  price_gross numeric GENERATED ALWAYS AS (price_net * (1 + vat_rate)) STORED,
  label       text    GENERATED ALWAYS AS (upper(name)) VIRTUAL
);
