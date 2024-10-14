import logging

import pandas as pd
from db_config import Connection


class ReadData(Connection):
    def __init__(self):
        super().__init__()

    def read_layer_bronze(self, table_name):

        try:
            q = "SELECT * FROM layer_bronze.bronze_data"
            self.cur.execute(q)
            data = self.cur.fetchall()
            col_names = [desc[0] for desc in self.cur.description]
            df = pd.DataFrame(data, columns=col_names)
            logging.info(f"Dados do schema {table_name} carregados com sucesso.")

            return df

        except Exception as e:
            logging.error(f"Erro ao ler dados: {e}")
            exit()
