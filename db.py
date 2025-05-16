import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user="postgres",
        password="ingreso-1"
    )
    return conn
