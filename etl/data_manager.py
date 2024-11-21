import csv
import logging
import os

import pandas as pd

from db.db_config import Connection


class DataManager:
    def __init__(self, file_path):
        self.file_path = '../data'
        self.path = file_path

    @staticmethod
    def delete_files(directory_path):
        try:
            files = os.listdir(directory_path)
            for file in files:
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            logging.info('Todos os arquivos foram excluídos do diretório.\n')
        except Exception as e:
            logging.error(f"Erro ao excluir arquivos: {e}")


class BaseLoader(Connection):
    def __init__(self):
        super().__init__()

    def insert_data(self, table, columns, values):
        placeholders = ', '.join(['%s'] * len(values))
        columns_str = ', '.join(columns)
        sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        try:
            self.execute(sql, values)
            self.commit()
        except Exception as e:
            logging.error(f"Erro ao inserir dados na tabela {table}: {e}")
            self.rollback()

    @staticmethod
    def load_csv_and_insert(filename, upload_class, insert_method_name):
        with open(filename, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            uploader = upload_class()

            # Obter o método de inserção dinamicamente
            insert_method = getattr(uploader, insert_method_name)

            for row in reader:
                insert_method(row)

    @staticmethod
    def process_row(row, schema):
        # Converte os dados de acordo com o schema esperado
        processed_data = []
        for column, conversion in schema.items():
            try:
                processed_data.append(conversion(row[column]))
            except Exception as e:
                logging.error(f"Erro ao processar coluna {column}: {e}")
                processed_data.append(None)
        return processed_data
