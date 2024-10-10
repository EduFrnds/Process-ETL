import csv
import logging

from db_config import Connection


class LoadToBronze(Connection):
    def __init__(self):
        super().__init__()

    def insert_layer_bronze(self, *args):

        sql = ("INSERT INTO layer_bronze.layer_bronze"
               "(equipment_id, production, hours_production, temperature, "
               "pressure, speed, vibration_level, maintenance_type, hours_maintenance)"
               "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        self.execute(sql, args)
        self.commit()

    def insert_csv(self, filename):
        try:
            data = csv.DictReader(open(filename, encoding='utf-8'))
            for row in data:
                equipment_id = int(row['equipment_id']) if 'equipment_id' in row and row['equipment_id'] else None
                production = float(row['production']) if 'production' in row and row['production'] else None
                hours_production = row['hours_production'] if 'hours_production' in row else None
                temperature = float(row['temperature']) if 'temperature' in row and row['temperature'] else None
                pressure = float(row['pressure']) if 'pressure' in row and row['pressure'] else None
                speed = float(row['speed']) if 'speed' in row and row['speed'] else None
                vibration_level = float(row['vibration_level']) if 'vibration_level' in row and row[
                    'vibration_level'] else None
                maintenance_type = row['maintenance_type'] if 'maintenance_type' in row else None
                hours_maintenance = float(row['hours_maintenance']) if 'hours_maintenance' in row and row[
                    'hours_maintenance'] else None

                self.insert_layer_bronze(equipment_id, production, hours_production, temperature, pressure, speed,
                                         vibration_level, maintenance_type, hours_maintenance)
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
