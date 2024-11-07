import logging

import pandas as pd
from db.db_config import Connection


class ReadDataBronze(Connection):
    def __init__(self):
        super().__init__()

    def read_layer_bronze(self, table_name):

        try:
            q = "SELECT * FROM layer_bronze.bronze_data"
            self.cur.execute(q)
            data = self.cur.fetchall()
            col_names = [desc[0] for desc in self.cur.description]
            df = pd.DataFrame(data, columns=col_names)
            logging.info(f'Leitura da tabela {table_name} feita com sucesso.')
            return df

        except Exception as e:
            logging.error(f"Erro ao ler dados: {e}")
            exit()
