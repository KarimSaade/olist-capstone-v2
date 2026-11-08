-- Olist Brazilian e-commerce schema (PostgreSQL 16)
-- Primary keys only. No secondary indexes are created here on purpose,
-- so the optimisation exercise in the notebook starts from a clean baseline.

DROP TABLE IF EXISTS order_reviews, order_items, orders, products,
                     sellers, customers, category_translation CASCADE;

CREATE TABLE customers (
    customer_id              VARCHAR(32) PRIMARY KEY,
    customer_unique_id       VARCHAR(32) NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city            TEXT,
    customer_state           CHAR(2)
);

CREATE TABLE sellers (
    seller_id              VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city            TEXT,
    seller_state           CHAR(2)
);

CREATE TABLE category_translation (
    product_category_name         TEXT PRIMARY KEY,
    product_category_name_english TEXT NOT NULL
);

CREATE TABLE products (
    product_id                 VARCHAR(32) PRIMARY KEY,
    product_category_name      TEXT,
    product_name_lenght        INTEGER,   -- original (misspelt) Olist column names kept
    product_description_lenght INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           INTEGER,
    product_length_cm          INTEGER,
    product_height_cm          INTEGER,
    product_width_cm           INTEGER
);

CREATE TABLE orders (
    order_id                      VARCHAR(32) PRIMARY KEY,
    customer_id                   VARCHAR(32) NOT NULL REFERENCES customers (customer_id),
    order_status                  VARCHAR(20) NOT NULL,
    order_purchase_timestamp      TIMESTAMP   NOT NULL,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE order_items (
    order_id            VARCHAR(32) NOT NULL REFERENCES orders (order_id),
    order_item_id       INTEGER     NOT NULL,
    product_id          VARCHAR(32) NOT NULL REFERENCES products (product_id),
    seller_id           VARCHAR(32) NOT NULL REFERENCES sellers (seller_id),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2) NOT NULL,
    freight_value       NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- review_id is NOT unique in the raw file (a review can cover several orders),
-- so the natural key is the (review_id, order_id) pair.
CREATE TABLE order_reviews (
    review_id               VARCHAR(32) NOT NULL,
    order_id                VARCHAR(32) NOT NULL REFERENCES orders (order_id),
    review_score            SMALLINT    NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);
