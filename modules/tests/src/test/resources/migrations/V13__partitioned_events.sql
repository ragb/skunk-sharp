-- RANGE-partitioned table (monthly) + default partition. The PK must include the partition key.
CREATE TABLE partitioned_events (
  id      bigserial NOT NULL,
  day     date      NOT NULL,
  kind    text      NOT NULL,
  payload text      NOT NULL,
  PRIMARY KEY (id, day)
) PARTITION BY RANGE (day);

CREATE TABLE partitioned_events_2026_01 PARTITION OF partitioned_events
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE partitioned_events_2026_02 PARTITION OF partitioned_events
  FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE partitioned_events_default PARTITION OF partitioned_events DEFAULT;
