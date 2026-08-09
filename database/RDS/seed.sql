-- Fake data for the demo (RDS PostgreSQL).
-- Run schema.sql first. Safe to re-run: it clears the tables before inserting.
--
-- Data is spread one order per day over the past ~2 years so there is a clear
-- mix of recent (OPEN) and old (CLOSED) records — just realistic sample data.

-- Local threshold used ONLY to make the fake data realistic:
-- orders older than this are marked CLOSED, newer ones OPEN.
-- This is NOT archival logic.
\set old_order_days 90

TRUNCATE TABLE order_items, orders, customers RESTART IDENTITY CASCADE;

-- 50 customers.
INSERT INTO customers (customer_id, full_name, email, country, created_at_utc)
SELECT
    'CUST-' || lpad(c::text, 4, '0'),
    'Customer ' || c,
    'customer' || c || '@example.com',
    (ARRAY['VN','US','JP','SG','DE'])[1 + (c % 5)],
    now() - ((c * 10) || ' days')::interval
FROM generate_series(1, 50) AS c;

-- 721 orders: one per day for the past ~2 years.
INSERT INTO orders (order_id, customer_id, status, amount, currency, created_at_utc, closed_at_utc)
SELECT
    'ORD-' || lpad(g::text, 6, '0'),
    'CUST-' || lpad((1 + (g % 50))::text, 4, '0'),
    CASE WHEN g > :old_order_days THEN 'CLOSED' ELSE 'OPEN' END,
    round((random() * 990 + 10)::numeric, 2),
    'USD',
    now() - (g || ' days')::interval,
    CASE WHEN g > :old_order_days
         THEN now() - (g || ' days')::interval + interval '2 days'
         ELSE NULL END
FROM generate_series(0, 720) AS g;

-- 1 to 3 line items per order.
INSERT INTO order_items (order_id, sku, quantity, unit_price)
SELECT
    o.order_id,
    'SKU-' || lpad(((random() * 200)::int)::text, 4, '0'),
    1 + (random() * 4)::int,
    round((random() * 200 + 5)::numeric, 2)
FROM orders o
CROSS JOIN generate_series(1, 1 + (random() * 2)::int) AS n;

-- Quick summary of the seeded rows.
SELECT
    count(*)                                        AS total_orders,
    count(*) FILTER (WHERE status = 'CLOSED')       AS closed_orders,
    count(*) FILTER (WHERE status = 'OPEN')         AS open_orders
FROM orders;
