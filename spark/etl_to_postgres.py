"""
ETL Pipeline: mock_data -> star/snowflake schema in PostgreSQL.

Run inside the spark container:
    spark-submit /home/jovyan/work/etl_to_postgres.py
"""

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, row_number, trim, year
from pyspark.sql.window import Window


PG_CONFIG = {
    "url": "jdbc:postgresql://postgres_lab:5432/bigdata_lab",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


CUSTOMER_COLUMNS = [
    ("sale_customer_id", "customer_id"),
    ("customer_first_name", "customer_first_name"),
    ("customer_last_name", "customer_last_name"),
    ("customer_age", "customer_age"),
    ("customer_email", "customer_email"),
    ("customer_country", "customer_country"),
    ("customer_postal_code", "customer_postal_code"),
    ("customer_pet_type", "customer_pet_type"),
    ("customer_pet_name", "customer_pet_name"),
    ("customer_pet_breed", "customer_pet_breed"),
]

SELLER_COLUMNS = [
    ("sale_seller_id", "seller_id"),
    ("seller_first_name", "seller_first_name"),
    ("seller_last_name", "seller_last_name"),
    ("seller_email", "seller_email"),
    ("seller_country", "seller_country"),
    ("seller_postal_code", "seller_postal_code"),
]

STORE_COLUMNS = [
    ("store_name", "store_name"),
    ("store_location", "store_location"),
    ("store_city", "store_city"),
    ("store_state", "store_state"),
    ("store_country", "store_country"),
    ("store_phone", "store_phone"),
    ("store_email", "store_email"),
]

SUPPLIER_COLUMNS = [
    ("supplier_name", "supplier_name"),
    ("supplier_contact", "supplier_contact"),
    ("supplier_email", "supplier_email"),
    ("supplier_phone", "supplier_phone"),
    ("supplier_address", "supplier_address"),
    ("supplier_city", "supplier_city"),
    ("supplier_country", "supplier_country"),
]

PRODUCT_COLUMNS = [
    ("sale_product_id", "product_id"),
    ("product_name", "product_name"),
    ("product_category", "product_category"),
    ("product_price", "product_price"),
    ("product_quantity", "product_quantity"),
    ("pet_category", "pet_category"),
    ("product_weight", "product_weight"),
    ("product_color", "product_color"),
    ("product_size", "product_size"),
    ("product_brand", "product_brand"),
    ("product_material", "product_material"),
    ("product_description", "product_description"),
    ("product_rating", "product_rating"),
    ("product_reviews", "product_reviews"),
    ("product_release_date", "product_release_date"),
    ("product_expiry_date", "product_expiry_date"),
]


def create_spark_session():
    return (
        SparkSession.builder.appName("ETL to PostgreSQL Star Schema")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def jdbc_properties():
    return {
        "user": PG_CONFIG["user"],
        "password": PG_CONFIG["password"],
        "driver": PG_CONFIG["driver"],
    }


def read_mock_data(spark):
    print("Reading mock_data from PostgreSQL...")
    df = spark.read.jdbc(url=PG_CONFIG["url"], table="mock_data", properties=jdbc_properties())
    print(f"mock_data rows: {df.count()}")
    return df


def cleaned_source(df):
    string_columns = {
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
        "product_name",
        "product_category",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
        "pet_category",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    }

    return df.select(
        *[
            trim(col(name)).alias(name) if name in string_columns else col(name)
            for name in df.columns
        ]
    ).cache()


def null_safe_condition(left_alias, right_alias, pairs):
    checks = [
        col(f"{left_alias}.{left_col}").eqNullSafe(col(f"{right_alias}.{right_col}"))
        for left_col, right_col in pairs
    ]
    return reduce(lambda a, b: a & b, checks)


def append_dimension_key(fact_df, dim_df, key_col, pairs, dim_alias):
    fact_alias = "f"
    condition = null_safe_condition(fact_alias, dim_alias, pairs)
    return (
        fact_df.alias(fact_alias)
        .join(dim_df.alias(dim_alias), condition, "left")
        .select(col(f"{fact_alias}.*"), col(f"{dim_alias}.{key_col}"))
    )


def add_surrogate_key(df, key_col, order_cols):
    window = Window.orderBy(*[col(name).asc_nulls_last() for name in order_cols])
    return df.withColumn(key_col, row_number().over(window))


def select_dimension(df, column_pairs):
    return df.select(*[col(src).alias(dst) for src, dst in column_pairs]).distinct()


def write_to_postgres(df, table_name):
    df.write.jdbc(
        url=PG_CONFIG["url"],
        table=table_name,
        mode="overwrite",
        properties=jdbc_properties(),
    )
    print(f"Wrote {df.count()} rows to {table_name}")


def execute_postgres_sql(spark, sql):
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName(PG_CONFIG["driver"])
    conn = jvm.java.sql.DriverManager.getConnection(
        PG_CONFIG["url"], PG_CONFIG["user"], PG_CONFIG["password"]
    )
    try:
        statement = conn.createStatement()
        statement.execute(sql)
        statement.close()
    finally:
        conn.close()


def create_primary_key(spark, table_name, key_col):
    try:
        execute_postgres_sql(
            spark,
            f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({key_col})",
        )
    except Exception as exc:
        print(f"Warning: could not create primary key for {table_name}: {exc}")


def create_dimensions(spark, source):
    print("Creating dimensions...")

    dim_customer = add_surrogate_key(
        select_dimension(source, CUSTOMER_COLUMNS),
        "customer_key",
        [dst for _, dst in CUSTOMER_COLUMNS],
    ).select("customer_key", *[dst for _, dst in CUSTOMER_COLUMNS])
    write_to_postgres(dim_customer, "dim_customer")

    dim_seller = add_surrogate_key(
        select_dimension(source, SELLER_COLUMNS),
        "seller_key",
        [dst for _, dst in SELLER_COLUMNS],
    ).select("seller_key", *[dst for _, dst in SELLER_COLUMNS])
    write_to_postgres(dim_seller, "dim_seller")

    dim_store = add_surrogate_key(
        select_dimension(source, STORE_COLUMNS),
        "store_key",
        [dst for _, dst in STORE_COLUMNS],
    ).select("store_key", *[dst for _, dst in STORE_COLUMNS])
    write_to_postgres(dim_store, "dim_store")

    dim_supplier = add_surrogate_key(
        select_dimension(source, SUPPLIER_COLUMNS),
        "supplier_key",
        [dst for _, dst in SUPPLIER_COLUMNS],
    ).select("supplier_key", *[dst for _, dst in SUPPLIER_COLUMNS])
    write_to_postgres(dim_supplier, "dim_supplier")

    product_source = source.select(
        *[col(src).alias(dst) for src, dst in PRODUCT_COLUMNS],
        *[col(src) for src, _ in SUPPLIER_COLUMNS],
    )
    product_with_supplier = append_dimension_key(
        product_source,
        dim_supplier,
        "supplier_key",
        SUPPLIER_COLUMNS,
        "sup",
    )
    dim_product = add_surrogate_key(
        product_with_supplier.select(
            *[dst for _, dst in PRODUCT_COLUMNS],
            "supplier_key",
        ).distinct(),
        "product_key",
        [dst for _, dst in PRODUCT_COLUMNS] + ["supplier_key"],
    ).select("product_key", *[dst for _, dst in PRODUCT_COLUMNS], "supplier_key")
    write_to_postgres(dim_product, "dim_product")

    dim_date = source.select("sale_date").where(col("sale_date").isNotNull()).distinct()
    dim_date = (
        add_surrogate_key(
            dim_date.withColumn("year", year("sale_date"))
            .withColumn("month", month("sale_date"))
            .withColumn("day", dayofmonth("sale_date"))
            .withColumn("quarter", quarter("sale_date"))
            .withColumn("day_of_week", date_format("sale_date", "EEEE")),
            "date_key",
            ["sale_date"],
        )
        .select("date_key", "sale_date", "year", "month", "day", "quarter", "day_of_week")
    )
    write_to_postgres(dim_date, "dim_date")

    for table_name, key_col in [
        ("dim_customer", "customer_key"),
        ("dim_seller", "seller_key"),
        ("dim_store", "store_key"),
        ("dim_supplier", "supplier_key"),
        ("dim_product", "product_key"),
        ("dim_date", "date_key"),
    ]:
        create_primary_key(spark, table_name, key_col)

    return dim_customer, dim_seller, dim_store, dim_supplier, dim_product, dim_date


def create_fact_sales(spark, source, dimensions):
    print("Creating fact_sales...")
    dim_customer, dim_seller, dim_store, dim_supplier, dim_product, dim_date = dimensions

    fact = source.select(
        "id",
        "sale_customer_id",
        *[src for src, _ in CUSTOMER_COLUMNS if src != "sale_customer_id"],
        "sale_seller_id",
        *[src for src, _ in SELLER_COLUMNS if src != "sale_seller_id"],
        *[src for src, _ in STORE_COLUMNS],
        "sale_product_id",
        *[src for src, _ in PRODUCT_COLUMNS if src != "sale_product_id"],
        *[src for src, _ in SUPPLIER_COLUMNS],
        "sale_date",
        "sale_quantity",
        "sale_total_price",
    )

    fact = append_dimension_key(fact, dim_customer, "customer_key", CUSTOMER_COLUMNS, "cust")
    fact = append_dimension_key(fact, dim_seller, "seller_key", SELLER_COLUMNS, "sell")
    fact = append_dimension_key(fact, dim_store, "store_key", STORE_COLUMNS, "store")
    fact = append_dimension_key(fact, dim_supplier, "supplier_key", SUPPLIER_COLUMNS, "sup")

    product_pairs = PRODUCT_COLUMNS + [("supplier_key", "supplier_key")]
    fact = append_dimension_key(fact, dim_product, "product_key", product_pairs, "prod")
    fact = append_dimension_key(fact, dim_date, "date_key", [("sale_date", "sale_date")], "dt")

    missing_keys = fact.where(
        col("customer_key").isNull()
        | col("seller_key").isNull()
        | col("store_key").isNull()
        | col("supplier_key").isNull()
        | col("product_key").isNull()
        | col("date_key").isNull()
    ).count()
    if missing_keys:
        raise RuntimeError(f"Cannot create fact_sales: {missing_keys} rows have missing dimension keys")

    fact_sales = fact.select(
        col("customer_key"),
        col("seller_key"),
        col("store_key"),
        col("product_key"),
        col("date_key"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("id").alias("original_id"),
    )
    fact_sales = add_surrogate_key(
        fact_sales,
        "sales_key",
        [
            "original_id",
            "customer_key",
            "seller_key",
            "store_key",
            "product_key",
            "date_key",
            "sale_quantity",
            "sale_total_price",
        ],
    ).select(
        "sales_key",
        "customer_key",
        "seller_key",
        "store_key",
        "product_key",
        "date_key",
        "sale_quantity",
        "sale_total_price",
        "original_id",
    )

    write_to_postgres(fact_sales, "fact_sales")
    create_primary_key(spark, "fact_sales", "sales_key")
    return fact_sales


def create_indexes(spark):
    print("Creating indexes...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_seller_id ON dim_seller(seller_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_store_name ON dim_store(store_name)",
        "CREATE INDEX IF NOT EXISTS idx_dim_supplier_name ON dim_supplier(supplier_name)",
        "CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_product_supplier ON dim_product(supplier_key)",
        "CREATE INDEX IF NOT EXISTS idx_dim_date_sale_date ON dim_date(sale_date)",
        "CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_seller ON fact_sales(seller_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_store ON fact_sales(store_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_key)",
        "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_sales(date_key)",
    ]
    for sql in indexes:
        try:
            execute_postgres_sql(spark, sql)
        except Exception as exc:
            print(f"Warning: could not run `{sql}`: {exc}")


def verify_results(spark):
    print("\n=== PostgreSQL verification ===")
    for table in [
        "mock_data",
        "dim_customer",
        "dim_seller",
        "dim_store",
        "dim_supplier",
        "dim_product",
        "dim_date",
        "fact_sales",
    ]:
        count = spark.read.jdbc(url=PG_CONFIG["url"], table=table, properties=jdbc_properties()).count()
        print(f"{table}: {count}")


def main():
    spark = create_spark_session()
    try:
        source = cleaned_source(read_mock_data(spark))
        dimensions = create_dimensions(spark, source)
        create_fact_sales(spark, source, dimensions)
        create_indexes(spark)
        verify_results(spark)
        print("\nPostgreSQL ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
