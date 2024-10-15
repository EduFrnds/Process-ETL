import csv
import logging

from db_config import Connection


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
    def load_csv(filename):
        try:
            with open(filename, encoding='utf-8') as csvfile:
                data = csv.DictReader(csvfile)
                return [row for row in data]
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo CSV {filename}: {e}")
            return None

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


class LoadToBronze(BaseLoader):
    def __init__(self):
        super().__init__()

    def insert_layer_bronze(self, row):
        schema = {
            'equipment_id': int,
            'production': float,
            'hours_production': str,
            'temperature': float,
            'pressure': float,
            'speed': float,
            'vibration_level': float,
            'operation_status': str,
            'maintenance_type': str,
            'hours_maintenance': float
        }
        values = self.process_row(row, schema)
        columns = list(schema.keys())
        self.insert_data('layer_bronze.bronze_data', columns, values)

    def insert_csv(self, filename):
        data = self.load_csv(filename)
        if data:
            for row in data:
                self.insert_layer_bronze(row)


class LoadToSilver(BaseLoader):
    def __init__(self):
        super().__init__()

    def insert_layer_silver(self, row):
        schema = {
            'equipment_id': int,
            'production': float,
            'maintenance_type': str,
            'month': int,
            'year': int,
            'maintenance_minutes': float,
            'production_minutes': float,
            'temperature_mean': float,
            'vibration_level_mean': float,
            'temperature_std': float,
            'vibration_level_std': float,
            'temperature_lsc': float,
            'temperature_lsi': float,
            'vibration_level_lsc': float,
            'vibration_level_lsi': float
        }
        values = self.process_row(row, schema)
        columns = list(schema.keys())
        self.insert_data('layer_silver.silver_data', columns, values)

    def insert_csv(self, filename):
        data = self.load_csv(filename)
        if data:
            for row in data:
                self.insert_layer_silver(row)
