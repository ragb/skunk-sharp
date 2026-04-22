CREATE TABLE sales (
  id      serial  PRIMARY KEY,
  year    int     NOT NULL,
  quarter int     NOT NULL,
  amount  int     NOT NULL
);
