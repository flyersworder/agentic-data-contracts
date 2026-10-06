CREATE TABLE customers (custid BIGINT PRIMARY KEY, name TEXT, tier TEXT, joined DATE);
CREATE TABLE orders (orderid BIGINT PRIMARY KEY, custid BIGINT, amount DOUBLE, qty INTEGER, status TEXT, details JSON);
INSERT INTO customers VALUES
  (1, 'Ann', 'gold', DATE '2024-01-05'),
  (2, 'Bob', 'silver', DATE '2024-02-10'),
  (3, 'Cy', 'gold', DATE '2024-03-15');
INSERT INTO orders VALUES
  (10, 1, 120.5, 3, 'shipped', '{"gift": true}'),
  (11, 1, 80.0, 1, 'returned', NULL),
  (12, 2, 200.0, 4, 'shipped', NULL),
  (13, 3, 15.25, 1, 'shipped', '{"gift": false}'),
  (14, 3, NULL, 2, 'pending', NULL);
