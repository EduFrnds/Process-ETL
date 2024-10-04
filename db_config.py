import os

import psycopg2 as pdb


class Config:
    def __init__(self):
        pass

    conexao = pdb.connect(
        database=os.getenv("POSTGRES_DB_NAME", default="postgres"),
        user=os.getenv("POSTGRES_DB_USER", default="postgres"),
        password=os.getenv("POSTGRES_DB_PASS", default="sql123"),
        host=os.getenv("POSTGRES_DB_HOST", default="localhost"),
        port=os.getenv("POSTGRES_DB_PORT", default="5432"),
    )


class Connection(Config):
    def __init__(self):
        super().__init__()
        try:
            self.conn = pdb.connect(**self.config['postgres'])
            self.cur = self.conn.cursor()

        except Exception as e:
            print("Erro na conexão", e)
            exit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()
        self.connection.close()

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.connection.commit()

    def fetchall(self):
        return self.cursor.fetchall()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


if __name__ == '__main__':
    conexao = pdb.connect
    print(f"{conexao}")
