# import os
#
# import psycopg2 as pdb
#
#
# class Config:
#     def __init__(self):
#         self.config = {
#             "postgres": {
#                 "user": "postgres",
#                 "password": "sql123",
#                 "host": "localhost",
#                 "port": "5432",
#                 "database": os.getenv('POSTGRES_DB_NAME')
#             }
#         }
#
#
# class Connection(Config):
#     def __init__(self):
#         super().__init__()
#         try:
#             self.conn = pdb.connect(**self.config['postgres'])
#             self.cur = self.conn.cursor()
#
#         except Exception as e:
#             print("Erro na conexão", e)
#             exit()
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_value, traceback):
#         self.commit()
#         self.connection.close()
#
#     @property
#     def connection(self):
#         return self.conn
#
#     @property
#     def cursor(self):
#         return self.cur
#
#     def commit(self):
#         self.connection.commit()
#
#     def fetchall(self):
#         return self.cursor.fetchall()
#
#     def execute(self, sql, params=None):
#         self.cursor.execute(sql, params or ())
#
#     def query(self, sql, params=None):
#         self.cursor.execute(sql, params or ())
#         return self.fetchall()
#
#
# if __name__ == '__main__':
#     conexao = pdb.connect
