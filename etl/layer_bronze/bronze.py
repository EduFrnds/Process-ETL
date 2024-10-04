import os

import pandas as pd
from sqlalchemy import create_engine
from db_config import Config


class LoadToBronze(Config):
    def __init__(self, csv_file_path, table_name, db_url):
        super().__init__()

        self.csv_file_path = "./data"
        self.table_name = table_name
        self.db_url = db_url

    def load_to_bronze(self, file_name):

        try:
            full_path = os.path.join(self.csv_file_path)

            if not os.path.exists(full_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {full_path}")

            data_csv = pd.read_parquet(f"{self.csv_file_path}/{file_name}")
            for row in data_csv:
                print(row)
        except Exception as e:
            print("Erro ao inserir dados na tabela", e)
