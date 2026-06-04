import json
import psycopg2
from psycopg2 import OperationalError

def test_postgres_connection(config):
    try:
        to_conn = psycopg2.connect(**config)
        print("Success in connecting to PostgreSQL")
        print(f"Using port: {config.get('port')}")

        cursor = to_conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print("PostgreSQL version: ", db_version[0])

        cursor.close()
        to_conn.close()
    except OperationalError as op_e:
        print(f"Error occurred in connecting to PostgreSQL: {op_e}")
    except Exception as e:
        print(f"Other error has occurred: {e}")

    
if __name__ == '__main__':
    source_config = {
        "host": "localhost",
        "port": "5433",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

    dest_config = {
        "host": "localhost",
        "port": "5434",
        "database": "postgres",
        "user": "postgres",
        "password": "postgres"
    }

    test_postgres_connection(source_config)
    test_postgres_connection(dest_config)

