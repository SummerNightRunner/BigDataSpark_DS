"""
ETL Pipeline: PostgreSQL star/snowflake schema -> Neo4j report nodes.

Run inside the spark container:
    spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/etl_to_neo4j.py
"""

import base64
import json
import urllib.request

from report_utils import build_reports, create_spark_session, normalize_df, row_to_json_dict


NEO4J_TX_URL = "http://neo4j:7474/db/neo4j/tx/commit"
NEO4J_AUTH = ("neo4j", "password")

REPORT_LABELS = {
    "product_sales_report": "ProductSalesReport",
    "customer_sales_report": "CustomerSalesReport",
    "time_sales_report": "TimeSalesReport",
    "store_sales_report": "StoreSalesReport",
    "supplier_sales_report": "SupplierSalesReport",
    "product_quality_report": "ProductQualityReport",
}


def neo4j_request(statements):
    auth = base64.b64encode(f"{NEO4J_AUTH[0]}:{NEO4J_AUTH[1]}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        NEO4J_TX_URL,
        data=json.dumps({"statements": statements}).encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def write_report(table_name, rows):
    label = REPORT_LABELS[table_name]
    neo4j_request(
        [
            {
                "statement": f"MATCH (n:{label}) DETACH DELETE n",
            }
        ]
    )

    prepared_rows = [
        {
            "record_id": str(index),
            "report": table_name,
            "payload": json.dumps(row_to_json_dict(row), ensure_ascii=False, sort_keys=True),
        }
        for index, row in enumerate(rows, start=1)
    ]

    for batch in chunks(prepared_rows, 1000):
        neo4j_request(
            [
                {
                    "statement": (
                        f"UNWIND $rows AS row "
                        f"CREATE (n:ReportRecord:{label} "
                        f"{{report: row.report, record_id: row.record_id, payload: row.payload}})"
                    ),
                    "parameters": {"rows": batch},
                }
            ]
        )
    print(f"Wrote {len(prepared_rows)} Neo4j nodes with label {label}")


def main():
    spark = create_spark_session("ETL to Neo4j Reports")
    try:
        for table_name, report_df in build_reports(spark).items():
            rows = normalize_df(report_df).collect()
            write_report(table_name, rows)
        print("\nNeo4j reports ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
