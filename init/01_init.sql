-- Таблица для импорта сырых данных
CREATE TABLE IF NOT EXISTS mock_data (
    id INTEGER,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_age INTEGER,
    customer_email VARCHAR(255),
    customer_country VARCHAR(255),
    customer_postal_code VARCHAR(255),
    customer_pet_type VARCHAR(255),
    customer_pet_name VARCHAR(255),
    customer_pet_breed VARCHAR(255),
    seller_first_name VARCHAR(255),
    seller_last_name VARCHAR(255),
    seller_email VARCHAR(255),
    seller_country VARCHAR(255),
    seller_postal_code VARCHAR(255),
    product_name VARCHAR(255),
    product_category VARCHAR(255),
    product_price DECIMAL(10, 2),
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price DECIMAL(12, 2),
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    store_city VARCHAR(255),
    store_state VARCHAR(255),
    store_country VARCHAR(255),
    store_phone VARCHAR(255),
    store_email VARCHAR(255),
    pet_category VARCHAR(255),
    product_weight DECIMAL(10, 2),
    product_color VARCHAR(255),
    product_size VARCHAR(255),
    product_brand VARCHAR(255),
    product_material VARCHAR(255),
    product_description TEXT,
    product_rating DECIMAL(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(255),
    supplier_contact VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(255),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(255),
    supplier_country VARCHAR(255)
);

-- Импорт всех CSV-файлов
\copy mock_data FROM '/data/MOCK_DATA.csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (1).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (2).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (3).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (4).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (5).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (6).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (7).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (8).csv' WITH (FORMAT csv, HEADER true);

\copy mock_data FROM '/data/MOCK_DATA (9).csv' WITH (FORMAT csv, HEADER true);