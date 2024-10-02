import os

import psycopg2

conexao = psycopg2.connect(
    database=os.getenv("POSTGRES_DB_NAME", default="postgres"),
    user=os.getenv("POSTGRES_DB_USER", default="postgres"),
    password=os.getenv("POSTGRES_DB_PASS", default="sql123"),
    host=os.getenv("POSTGRES_DB_HOST", default="localhost"),
    port=os.getenv("POSTGRES_DB_PORT", default="5432"),
)
print(conexao.info)
print(conexao.status)

if __name__ == '__main__':
    conexao = psycopg2.connect

