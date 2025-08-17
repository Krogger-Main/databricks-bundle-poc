-- Uses UC if available, else hive_metastore
-- Change the catalog/schema in conf/*.yml (defaults provided)

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.sales_raw
(
  order_id     STRING,
  order_date   DATE,
  store_id     STRING,
  item_id      STRING,
  qty          INT,
  price        DECIMAL(10,2)
)
USING DELTA;

INSERT INTO ${catalog}.${schema}.sales_raw VALUES
('A001', DATE('2024-07-01'), 'S1', 'I1', 2, 10.50),
('A002', DATE('2024-07-02'), 'S1', 'I2', 1,  5.00),
('A003', DATE('2024-07-03'), 'S2', 'I1', 3, 10.50);
