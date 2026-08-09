-- Schema for the demo (RDS PostgreSQL).
-- Relational model: customers 1--* orders 1--* order_items.

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id     text PRIMARY KEY,
    full_name       text        NOT NULL,
    email           text        NOT NULL,
    country         text        NOT NULL,
    created_at_utc  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id        text PRIMARY KEY,
    customer_id     text        NOT NULL REFERENCES customers(customer_id),
    status          text        NOT NULL,          -- OPEN | CLOSED | CANCELLED
    amount          numeric(18,2) NOT NULL,
    currency        text        NOT NULL DEFAULT 'USD',
    created_at_utc  timestamptz NOT NULL,
    closed_at_utc   timestamptz                    -- set only for terminal orders
);

CREATE TABLE order_items (
    order_item_id   bigserial PRIMARY KEY,
    order_id        text        NOT NULL REFERENCES orders(order_id),
    sku             text        NOT NULL,
    quantity        integer     NOT NULL,
    unit_price      numeric(18,2) NOT NULL
);

-- Common lookup indexes.
CREATE INDEX idx_orders_status_closed ON orders (status, closed_at_utc);
CREATE INDEX idx_order_items_order     ON order_items (order_id);
