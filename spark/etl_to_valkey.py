"""
ETL Pipeline: PostgreSQL star/snowflake schema -> Valkey records.

Run inside the spark container:
    spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/etl_to_valkey.py
"""

import json
import socket

from report_utils import build_reports, create_spark_session, normalize_df, row_to_json_dict


VALKEY_HOST = "valkey"
VALKEY_PORT = 6379


def encode_command(*parts):
    encoded = [str(part).encode("utf-8") for part in parts]
    command = [f"*{len(encoded)}\r\n".encode("utf-8")]
    for part in encoded:
        command.append(f"${len(part)}\r\n".encode("utf-8"))
        command.append(part)
        command.append(b"\r\n")
    return b"".join(command)


def read_line(sock):
    chunks = []
    while True:
        char = sock.recv(1)
        if not char:
            raise ConnectionError("Valkey closed the connection")
        chunks.append(char)
        if len(chunks) >= 2 and chunks[-2:] == [b"\r", b"\n"]:
            return b"".join(chunks[:-2]).decode("utf-8")


def read_reply(sock):
    prefix = sock.recv(1)
    if prefix in (b"+", b"-"):
        line = read_line(sock)
        if prefix == b"-":
            raise RuntimeError(f"Valkey error: {line}")
        return line
    if prefix == b":":
        return int(read_line(sock))
    if prefix == b"$":
        length = int(read_line(sock))
        if length == -1:
            return None
        data = sock.recv(length)
        sock.recv(2)
        return data.decode("utf-8")
    if prefix == b"*":
        length = int(read_line(sock))
        return [read_reply(sock) for _ in range(length)]
    raise RuntimeError(f"Unexpected Valkey reply prefix: {prefix!r}")


def execute(sock, *parts):
    sock.sendall(encode_command(*parts))
    return read_reply(sock)


def write_report(sock, table_name, rows):
    pipe = []
    for index, row in enumerate(rows, start=1):
        key = f"report:{table_name}:{index}"
        payload = json.dumps(row_to_json_dict(row), ensure_ascii=False, sort_keys=True)
        pipe.append(encode_command("SET", key, payload))
    pipe.append(encode_command("SET", f"report:{table_name}:count", len(rows)))
    sock.sendall(b"".join(pipe))
    for _ in pipe:
        read_reply(sock)
    print(f"Wrote {len(rows)} records to Valkey prefix report:{table_name}:*")


def main():
    spark = create_spark_session("ETL to Valkey Reports")
    try:
        reports = build_reports(spark)
        with socket.create_connection((VALKEY_HOST, VALKEY_PORT), timeout=30) as sock:
            execute(sock, "FLUSHDB")
            for table_name, report_df in reports.items():
                rows = normalize_df(report_df).collect()
                write_report(sock, table_name, rows)
        print("\nValkey reports ETL finished successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
