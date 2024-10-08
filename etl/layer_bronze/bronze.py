import csv

from db_config import Connection


class LoadToBronze(Connection):
    def __init__(self):
        super().__init__()

    def insert_layer_bronze(self, *args):
        try:
            sql = ("INSERT INTO layer_bronze.table_bronze"
                   "(equipment_id, production, timestamp, temperature, "
                   "pressure, speed, vibration_level, maintenance_type, hours_maintenance)"
                   "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self.execute(sql, args)
            self.commit()
        except Exception as e:
            print("Erro ao inserir", e)

    def insert_csv(self, filename):
        try:
            data = csv.DictReader(open(filename, encoding='utf-8'))
            for row in data:
                # Convertendo os dados para os tipos corretos, se necessário
                equipment_id = int(row['equipment_id'])
                production = float(row['production'])
                timestamp = row['timestamp']
                temperature = float(row['temperature'])
                pressure = float(row['pressure'])
                speed = float(row['speed'])
                vibration_level = float(row['vibration_level'])
                maintenance_type = row['maintenance_type']
                hours_maintenance = float(row['hours_maintenance'])

                self.insert_layer_bronze(equipment_id, production, timestamp, temperature, pressure, speed,
                                         vibration_level, maintenance_type, hours_maintenance)
        except Exception as e:
            print("Erro ao inserir", e)
