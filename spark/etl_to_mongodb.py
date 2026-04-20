"""
ETL Pipeline: PostgreSQL star/snowflake schema -> MongoDB report collections.

Run inside the spark container:
    spark-submit \
      --packages org.postgresql:postgresql:42.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
      /home/jovyan/work/etl_to_mongodb.py
"""

from report_utils import build_reports, create_spark_session, normalize_df


MONGO_URI = "mongodb://root:root@mongodb:27017"
MONGO_DATABASE = "reports"


def write_mongodb(df, collection_name):
    (
        normalize_df(df)
        .write.format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.write.connection.uri", MONGO_URI)
        .option("spark.mongodb.write.database", MONGO_DATABASE)
        .option("spark.mongodb.write.collection", collection_name)
        .save()
    )
    print(f"Wrote {df.count()} documents to MongoDB collection {collection_name}")


def main():
    spark = create_spark_session("ETL to MongoDB Reports")
    try:
        for collection_name, report_df in build_reports(spark).items():
            write_mongodb(report_df, collection_name)
        print("\nMongoDB reports ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
