import datetime
import random
from faker import Faker

from etl.data_manager import DataManager


class BaseDataGenerator:
    def __init__(self, data_path):
        self.fake = Faker('pt-BR')
        self.data_manager = DataManager(data_path)


class EquipmentProductionDataGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__('./data')

    def generate_data_equipments(self, r, headers):  # r=records
        """Generate data for equipaments"""

        start_date = datetime.datetime.now() - datetime.timedelta(days=365)
        end_date = datetime.datetime.now()

        equipment_data = []

        for _ in range(r):
            data = {
                "equipment_id": random.choice(["1", "2", "3"]),
                "production": round(random.uniform(300.0, 500.0), 2),  # Produção entre 300 e 500 kg/h
                "hours_production": self.fake.date_time_between(start_date=start_date, end_date=end_date),
                # "hours_production": self.fake.date_time_between(start_date='-1d', end_date='now'),
                "temperature": round(random.uniform(150.0, 250.0), 2),  # Temperatura entre 150°C e 250°C
                "pressure": round(random.uniform(100.0, 300.0), 2),  # Pressão entre 100 e 300 PSI
                "speed": round(random.uniform(50.0, 120.0), 2),  # Velocidade entre 50 e 120 RPM
                "vibration_level": round(random.uniform(0.1, 5.0), 2),  # Nível de vibração entre 0.1 e 5.0 mm/s
                "operation_status": random.choice(["ON", "OFF"])  # Equipamento ligado/desligado
            }
            equipment_data.append(data)

        self.data_manager.save_to_csv(equipment_data, 'equipments', headers)
        return equipment_data


class EquipmentMaintenanceDataGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__('./data')

    def generate_data_maintenances(self, r, headers):
        """Generate data for maintenances"""
        maintenance_data = []

        for _ in range(r):
            hours_worked = random.uniform(50, 200)  # Exemplo: entre 50 e 200 horas

            maintenance_data.append({
                "equipment_id": random.choice(["1", "2", "3"]),
                "maintenance_type": random.choice(["Repair", "Maintenance"]),
                "hours_maintenance": round(hours_worked, 2),
            })
        self.data_manager.save_to_csv(maintenance_data, 'maintenances', headers)
        return maintenance_data
