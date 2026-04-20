"""
ETL Pipeline: PostgreSQL star/snowflake schema -> Cassandra report tables.

Run inside the spark container:
    spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/etl_to_cassandra.py
"""

import json
import socket
import struct
import time

from report_utils import build_reports, create_spark_session, normalize_df, row_to_json_dict


CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "reports"


def cql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


class CassandraClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.stream = 0
        self.sock = None

    def connect(self):
        last_error = None
        for _ in range(30):
            try:
                self.sock = socket.create_connection((self.host, self.port), timeout=30)
                self.startup()
                return
            except OSError as exc:
                last_error = exc
                time.sleep(5)
        raise ConnectionError(f"Could not connect to Cassandra: {last_error}")

    def close(self):
        if self.sock:
            self.sock.close()

    def next_stream(self):
        self.stream = (self.stream + 1) % 32767
        return self.stream

    def send_frame(self, opcode, body):
        stream = self.next_stream()
        header = struct.pack(">BBhBI", 0x04, 0x00, stream, opcode, len(body))
        self.sock.sendall(header + body)
        return self.read_frame()

    def read_exact(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise ConnectionError("Cassandra closed the connection")
            data += chunk
        return data

    def read_frame(self):
        header = self.read_exact(9)
        _version, _flags, _stream, opcode, length = struct.unpack(">BBhBI", header)
        body = self.read_exact(length)
        if opcode == 0x00:
            code = struct.unpack(">i", body[:4])[0]
            message_len = struct.unpack(">H", body[4:6])[0]
            message = body[6 : 6 + message_len].decode("utf-8")
            raise RuntimeError(f"Cassandra error {code}: {message}")
        return opcode, body

    @staticmethod
    def write_string(value):
        raw = value.encode("utf-8")
        return struct.pack(">H", len(raw)) + raw

    @staticmethod
    def write_long_string(value):
        raw = value.encode("utf-8")
        return struct.pack(">I", len(raw)) + raw

    def startup(self):
        body = (
            struct.pack(">H", 1)
            + self.write_string("CQL_VERSION")
            + self.write_string("3.0.0")
        )
        opcode, _body = self.send_frame(0x01, body)
        if opcode != 0x02:
            raise RuntimeError(f"Unexpected Cassandra STARTUP response opcode: {opcode}")

    def execute(self, query):
        body = self.write_long_string(query) + struct.pack(">HB", 0x0001, 0x00)
        opcode, _body = self.send_frame(0x07, body)
        if opcode != 0x08:
            raise RuntimeError(f"Unexpected Cassandra QUERY response opcode: {opcode}")


def create_schema(client):
    client.execute(
        "CREATE KEYSPACE IF NOT EXISTS reports "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    for table_name in [
        "product_sales_report",
        "customer_sales_report",
        "time_sales_report",
        "store_sales_report",
        "supplier_sales_report",
        "product_quality_report",
    ]:
        client.execute(
            f"CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{table_name} "
            "(id text PRIMARY KEY, payload text)"
        )
        client.execute(f"TRUNCATE {CASSANDRA_KEYSPACE}.{table_name}")


def write_report(client, table_name, rows):
    prepared = []
    for index, row in enumerate(rows, start=1):
        payload = json.dumps(row_to_json_dict(row), ensure_ascii=False, sort_keys=True)
        prepared.append((str(index), payload))

    for start in range(0, len(prepared), 50):
        statements = []
        for record_id, payload in prepared[start : start + 50]:
            statements.append(
                f"INSERT INTO {CASSANDRA_KEYSPACE}.{table_name} (id, payload) "
                f"VALUES ({cql_string(record_id)}, {cql_string(payload)})"
            )
        client.execute("BEGIN UNLOGGED BATCH " + "; ".join(statements) + "; APPLY BATCH")
    print(f"Wrote {len(prepared)} rows to Cassandra table {CASSANDRA_KEYSPACE}.{table_name}")


def main():
    spark = create_spark_session("ETL to Cassandra Reports")
    client = CassandraClient(CASSANDRA_HOST, CASSANDRA_PORT)
    try:
        client.connect()
        create_schema(client)
        for table_name, report_df in build_reports(spark).items():
            rows = normalize_df(report_df).collect()
            write_report(client, table_name, rows)
        print("\nCassandra reports ETL finished successfully.")
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    main()
