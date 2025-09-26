CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL
);

CREATE TABLE products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    unit_price REAL NOT NULL,
    vat_code TEXT NOT NULL
    --reorder_point INTEGER NOT NULL
);

CREATE TABLE stock_levels (
    sku TEXT NOT NULL,
    warehouse TEXT NOT NULL,
    qty_on_hand INTEGER NOT NULL,
    PRIMARY KEY (sku, warehouse)
);

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_lines (
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    PRIMARY KEY (order_id, sku),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (sku) REFERENCES products(sku)
);
