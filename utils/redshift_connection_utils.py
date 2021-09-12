"""
Wrapper for managing connections to Redshift.
"""

import psycopg2
import sys

from utils import credential_manager_utils as credential_manager


_REDSHIFT_CREDENTIALS = credential_manager.read("redshift_creds")
_SAVED_CONNECTIONS = {}


def _get_credentials(db: str = "dev") -> tuple:
    """Access AWS Redshift Credentials."""

    host = f"{_REDSHIFT_CREDENTIALS['HOST']}"
    db = f"{_REDSHIFT_CREDENTIALS['DATABASE']}"
    port = f"{_REDSHIFT_CREDENTIALS['PORT']}"
    username = f"{_REDSHIFT_CREDENTIALS['USERNAME']}"
    password = f"{_REDSHIFT_CREDENTIALS['PASSWORD']}"

    return host, db, port, username, password


def _get_connection(db: str = "dev") -> dict:
    """Create Connection to Redshift Using psycopg2."""

    global _SAVED_CONNECTIONS
    key = f"db={db}"

    if key not in _SAVED_CONNECTIONS:
        (host, dbname, port, username, password) = _get_credentials(db)

        _SAVED_CONNECTIONS[key] = psycopg2.connect(
            dbname=dbname, host=host, port=port, user=username, password=password
        )

    return _SAVED_CONNECTIONS[key]


def execute(sql: str, quiet: bool = False, db: str = "dev") -> list:
    """Execute an SQL statement and return the results."""

    connection = _get_connection(db)

    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql)
            out = cursor.fetchall()
    except Exception as e:
        if "no results to fetch" in str(e):
            cursor.close()
            return []
        if not quiet:
            banner = "-" * 70
            sys.stderr.write(
                f"\n{banner}\nGot an error executing SQL statement!\nerror: {e}\nsql: {sql}\n{banner}\n"
            )
            sys.stderr.flush()
        cursor.close()
        raise

    return out


def close(db: str) -> None:
    """Close down any shared connections."""

    global _SAVED_CONNECTIONS
    key = f"db={db}"

    if key in _SAVED_CONNECTIONS:
        _SAVED_CONNECTIONS[key].close()
        del _SAVED_CONNECTIONS[key]
