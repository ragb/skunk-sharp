-- Non-unique indexes for the schema validator's index check (#94).
CREATE TABLE ledger (
  id           bigint PRIMARY KEY,
  household_id uuid   NOT NULL,
  booking_date date   NOT NULL,
  account      text
);

CREATE INDEX ledger_household_booking_idx ON ledger (household_id, booking_date DESC, id DESC);
CREATE INDEX ledger_account_linked_idx    ON ledger (account) WHERE account IS NOT NULL;
CREATE INDEX ledger_booking_idx           ON ledger (booking_date);
