"""Shared Crossref endpoint guard; connecting never submits or fetches work."""
import os
import psycopg2
from psycopg2.extensions import parse_dsn


def connect_pg18(dsn="", **kwargs):
    params = parse_dsn(dsn or "")
    params.update({key: value for key, value in kwargs.items() if value is not None})
    for key, environment, default in [
        ("dbname", "PGDATABASE", "crossref"),
        ("host", "PGHOST", "/var/run/postgresql"),
        ("port", "PGPORT", "5433"),
    ]:
        params.setdefault(key, os.getenv(environment) or default)
    if "user" not in params and os.getenv("PGUSER"):
        params["user"] = os.environ["PGUSER"]
    conn = psycopg2.connect(**params)
    expected_database = os.getenv("LIG_EXPECTED_DATABASE", "crossref")
    expected_user = params.get("user", conn.info.user)
    actual = (conn.info.dbname, conn.info.user, conn.info.port, conn.server_version // 10000)
    if actual != (expected_database, expected_user, 5433, 18):
        conn.close()
        raise RuntimeError("Language analysis database identity mismatch; refusing processing")
    return conn


if __name__ == "__main__":
    conn = connect_pg18()
    try:
        print(f"Verified {conn.info.dbname} on port {conn.info.port}, PostgreSQL {conn.server_version // 10000}")
    finally:
        conn.close()
