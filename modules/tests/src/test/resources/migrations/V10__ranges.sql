CREATE TABLE bookings (
  id      serial  PRIMARY KEY,
  period  daterange NOT NULL
);

CREATE TABLE reservations (
  id      serial  PRIMARY KEY,
  slot    tstzrange NOT NULL
);
