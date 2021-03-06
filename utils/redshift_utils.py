"""
Wrapper for prepping CSVs for Redshift and generating table schemas.
"""

import csv

from utils import redshift_connection_utils as rconn


def check_schema_exists(schema_name: str, db: str) -> bool:
    """Return True if the given schema exists, False otherwise."""

    sql = f"""
    SELECT EXISTS (
        SELECT nspname FROM pg_namespace
        WHERE nspname = '{schema_name}'
    );
    """
    out = rconn.execute(sql=sql, db=db)
    return out[0][0]


def check_table_exists(schema_name: str, table_name: str, db: str) -> bool:
    """Return True if the given table exists, False otherwise."""

    sql = f"""
    SELECT EXISTS (
        SELECT tablename FROM pg_tables
        WHERE schemaname = '{schema_name}' AND tablename = '{table_name}'
    );
    """
    out = rconn.execute(sql=sql, db=db)
    return out[0][0]


def get_schema(source_dir: str, source_file: str, dest_table: str) -> str:
    """Used to generate a Redshift-friendly table schema based on the CSV file."""

    with open(f"{source_dir}/{source_file}", "r") as f:
        schema = f.read().strip()

    sql = f"DROP TABLE IF EXISTS {dest_table}; CREATE TABLE {dest_table} ("
    sql = sql + schema + "); COMMIT;"

    return sql


def get_num_rows(schema_name: str, table_name: str, db: str) -> None:
    """Count the rows in a Redshift table."""

    rconn.execute("BEGIN;")
    num_rows = rconn.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name};", db=db)[0][0]
    rconn.execute("COMMIT;")
    print(f"Found {num_rows} total rows in transferred table: {schema_name}.{table_name}")
