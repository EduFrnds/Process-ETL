import csv
import logging

from db_config import Connection


class LoadToBronze(Connection):
    def __init__(self):
        super().__init__()

    def insert_layer_bronze(self, *args):

        sql = ("INSERT INTO layer_bronze.bronze_data"
               "(equipment_id, production, hours_production, temperature,"
               "pressure, speed, vibration_level,operation_status, maintenance_type, hours_maintenance)"
               "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        self.execute(sql, args)
        self.commit()

    def insert_csv(self, filename):
        try:
            data = csv.DictReader(open(filename, encoding='utf-8'))
            for row in data:
                equipment_id = int(row['equipment_id'])
                production = float(row['production'])
                hours_production = row['hours_production']
                temperature = float(row['temperature'])
                pressure = float(row['pressure'])
                speed = float(row['speed'])
                vibration_level = float(row['vibration_level'])
                operation_status = row['operation_status']
                maintenance_type = row['maintenance_type']
                hours_maintenance = float(row['hours_maintenance'])

                self.insert_layer_bronze(equipment_id, production, hours_production, temperature, pressure, speed,
                                         vibration_level, operation_status, maintenance_type, hours_maintenance)
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
