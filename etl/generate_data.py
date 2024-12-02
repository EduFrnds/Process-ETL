import datetime
import random
import pandas as pd
from faker import Faker


class EquipmentProductionDataGenerator:
    def __init__(self):
        self.fake = Faker()

    def generate_data_equipments(self, r, headers, start_year, end_year):

        # Gera os dados de produção para os equipamentos
        equipment_ids = ["1", "2", "3"]
        equipment_data = []
        total_records = 0

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                for equipment_id in equipment_ids:
                    # Verifica se atingiu o limite total de registros
                    if r and total_records >= r:
                        break

                    # Gera os dados para o equipamento
                    data = self._generate_equipment_data(equipment_id, year, month)
                    equipment_data.append(data)
                    total_records += 1

        # Converte para DataFrame e salva no CSV
        equipment_df = pd.DataFrame(equipment_data, columns=headers)
        equipment_df.to_csv('./data/equipments.csv', index=False)

    def _generate_equipment_data(self, equipment_id, year, month):

        # Gera os dados para o equipamento
        production = round(random.uniform(0.0, 350.0), 2)
        operation_status = random.choice(["ON", "OFF"])
        hours_worked = round(random.uniform(1, 8), 2)
        maintenance_type = "Maintenance" if production <= 50.0 else "Repair"

        return {
            "equipment_id": equipment_id,
            "production": production,
            "hours_production": self.fake.date_time_between(
                start_date=datetime.datetime(year, month, 1),
                end_date=datetime.datetime(year, month, 28, 23, 59, 59)
            ),
            "temperature": round(random.uniform(180.0, 250.0), 2),
            "pressure": round(random.uniform(0.5, 3.0), 2),
            "speed": round(random.uniform(0.5, 120.0), 2),
            "vibration_level": round(random.uniform(0.1, 2.0), 2),
            "operation_status": operation_status,
            "maintenance_type": maintenance_type,
            "hours_maintenance": hours_worked,
        }
