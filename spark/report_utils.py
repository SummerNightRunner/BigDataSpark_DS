from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    corr,
    count,
    countDistinct,
    dense_rank,
    lag,
    lit,
    round,
    sum as spark_sum,
)
from pyspark.sql.window import Window


PG_CONFIG = {
    "url": "jdbc:postgresql://postgres_lab:5432/bigdata_lab",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}


REPORT_TABLES = [
    "product_sales_report",
    "customer_sales_report",
    "time_sales_report",
    "store_sales_report",
    "supplier_sales_report",
    "product_quality_report",
]


def create_spark_session(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pg_properties():
    return {
        "user": PG_CONFIG["user"],
        "password": PG_CONFIG["password"],
        "driver": PG_CONFIG["driver"],
    }


def read_pg_table(spark, table_name):
    return spark.read.jdbc(url=PG_CONFIG["url"], table=table_name, properties=pg_properties()).cache()


def load_sales_mart(spark):
    fact_sales = read_pg_table(spark, "fact_sales")
    dim_customer = read_pg_table(spark, "dim_customer")
    dim_seller = read_pg_table(spark, "dim_seller")
    dim_store = read_pg_table(spark, "dim_store")
    dim_supplier = read_pg_table(spark, "dim_supplier")
    dim_product = read_pg_table(spark, "dim_product")
    dim_date = read_pg_table(spark, "dim_date")

    sales = (
        fact_sales.join(dim_customer, "customer_key", "inner")
        .join(dim_seller, "seller_key", "inner")
        .join(dim_store, "store_key", "inner")
        .join(dim_product, "product_key", "inner")
        .join(dim_supplier, "supplier_key", "inner")
        .join(dim_date, "date_key", "inner")
        .select(
            "sales_key",
            "sale_quantity",
            "sale_total_price",
            "customer_key",
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_country",
            "seller_key",
            "store_key",
            "store_name",
            "store_city",
            "store_country",
            "product_key",
            "product_id",
            "product_name",
            "product_category",
            "product_price",
            "product_rating",
            "product_reviews",
            "supplier_key",
            "supplier_name",
            "supplier_city",
            "supplier_country",
            "date_key",
            "sale_date",
            "year",
            "month",
            "quarter",
        )
    ).cache()
    return sales, dim_customer


def product_sales_report(sales):
    rank_window = Window.orderBy(col("total_quantity_sold").desc(), col("total_revenue").desc())
    return (
        sales.groupBy("product_key", "product_id", "product_name", "product_category")
        .agg(
            spark_sum("sale_quantity").alias("total_quantity_sold"),
            round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
            count("sales_key").alias("sales_count"),
            round(avg("product_rating"), 2).alias("avg_rating"),
            spark_sum("product_reviews").alias("total_reviews"),
        )
        .withColumn("sales_rank", dense_rank().over(rank_window))
        .orderBy("sales_rank", "product_key")
    )


def customer_sales_report(sales, dim_customer):
    country_distribution = dim_customer.groupBy("customer_country").agg(
        countDistinct("customer_key").alias("country_customer_count")
    )
    rank_window = Window.orderBy(col("total_purchase_amount").desc(), col("orders_count").desc())
    return (
        sales.groupBy(
            "customer_key",
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_country",
        )
        .agg(
            round(spark_sum("sale_total_price"), 2).alias("total_purchase_amount"),
            count("sales_key").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_check"),
        )
        .join(country_distribution, "customer_country", "left")
        .withColumn("purchase_rank", dense_rank().over(rank_window))
        .orderBy("purchase_rank", "customer_key")
    )


def time_sales_report(sales):
    period_window = Window.orderBy("year", "month")
    return (
        sales.groupBy("year", "month")
        .agg(
            round(spark_sum("sale_total_price"), 2).alias("monthly_revenue"),
            spark_sum("sale_quantity").alias("monthly_quantity_sold"),
            count("sales_key").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_order_amount"),
        )
        .withColumn("previous_month_revenue", lag("monthly_revenue").over(period_window))
        .withColumn("revenue_delta", round(col("monthly_revenue") - col("previous_month_revenue"), 2))
        .withColumn("period", lit("month"))
        .orderBy("year", "month")
    )


def store_sales_report(sales):
    city_distribution = sales.groupBy("store_city", "store_country").agg(
        count("sales_key").alias("city_country_sales_count"),
        round(spark_sum("sale_total_price"), 2).alias("city_country_revenue"),
    )
    rank_window = Window.orderBy(col("total_revenue").desc(), col("orders_count").desc())
    return (
        sales.groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
            spark_sum("sale_quantity").alias("total_quantity_sold"),
            count("sales_key").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_check"),
        )
        .join(city_distribution, ["store_city", "store_country"], "left")
        .withColumn("store_revenue_rank", dense_rank().over(rank_window))
        .orderBy("store_revenue_rank", "store_key")
    )


def supplier_sales_report(sales):
    country_distribution = sales.groupBy("supplier_country").agg(
        count("sales_key").alias("supplier_country_sales_count"),
        round(spark_sum("sale_total_price"), 2).alias("supplier_country_revenue"),
    )
    rank_window = Window.orderBy(col("total_revenue").desc(), col("orders_count").desc())
    return (
        sales.groupBy("supplier_key", "supplier_name", "supplier_city", "supplier_country")
        .agg(
            round(spark_sum("sale_total_price"), 2).alias("total_revenue"),
            spark_sum("sale_quantity").alias("total_quantity_sold"),
            count("sales_key").alias("orders_count"),
            round(avg("sale_total_price"), 2).alias("avg_check"),
            round(avg("product_price"), 2).alias("avg_product_price"),
        )
        .join(country_distribution, "supplier_country", "left")
        .withColumn("supplier_revenue_rank", dense_rank().over(rank_window))
        .orderBy("supplier_revenue_rank", "supplier_key")
    )


def product_quality_report(product_report):
    correlation = product_report.select(
        corr(col("avg_rating").cast("double"), col("total_quantity_sold").cast("double")).alias(
            "rating_sales_quantity_correlation"
        )
    )
    best_rating_window = Window.orderBy(col("avg_rating").desc(), col("total_reviews").desc())
    worst_rating_window = Window.orderBy(col("avg_rating").asc(), col("total_reviews").desc())
    reviews_window = Window.orderBy(col("total_reviews").desc(), col("total_quantity_sold").desc())

    return (
        product_report.crossJoin(correlation)
        .withColumn("best_rating_rank", dense_rank().over(best_rating_window))
        .withColumn("worst_rating_rank", dense_rank().over(worst_rating_window))
        .withColumn("reviews_rank", dense_rank().over(reviews_window))
        .select(
            "product_key",
            "product_id",
            "product_name",
            "product_category",
            "avg_rating",
            "total_reviews",
            "total_quantity_sold",
            "total_revenue",
            "rating_sales_quantity_correlation",
            "best_rating_rank",
            "worst_rating_rank",
            "reviews_rank",
        )
        .orderBy("best_rating_rank", "product_key")
    )


def build_reports(spark):
    sales, dim_customer = load_sales_mart(spark)
    product_report = product_sales_report(sales)
    return {
        "product_sales_report": product_report,
        "customer_sales_report": customer_sales_report(sales, dim_customer),
        "time_sales_report": time_sales_report(sales),
        "store_sales_report": store_sales_report(sales),
        "supplier_sales_report": supplier_sales_report(sales),
        "product_quality_report": product_quality_report(product_report),
    }


def normalize_df(df):
    return df.na.fill(0).na.fill("")


def json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def row_to_json_dict(row):
    return {key: json_safe(value) for key, value in row.asDict(recursive=True).items()}
