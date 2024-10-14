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

    def generate_data_equipments(self, r, headers):
        """Generate data for equipaments"""

        start_date = datetime.datetime.now() - datetime.timedelta(days=365)
        end_date = datetime.datetime.now()

        equipment_data = []

        for _ in range(r):
            hours_worked = random.uniform(50, 200)
            data = {
                "equipment_id": random.choice(["1", "2", "3"]),
                "production": round(random.uniform(300.0, 500.0), 2),
                "hours_production": self.fake.date_time_between(start_date=start_date, end_date=end_date),
                "temperature": round(random.uniform(150.0, 250.0), 2),
                "pressure": round(random.uniform(100.0, 300.0), 2),
                "speed": round(random.uniform(50.0, 120.0), 2),
                "vibration_level": round(random.uniform(0.1, 5.0), 2),
                "operation_status": random.choice(["ON", "OFF"]),
                "maintenance_type": random.choice(["Repair", "Maintenance"]),
                "hours_maintenance": round(hours_worked, 2),
            }
            equipment_data.append(data)

        self.data_manager.save_to_csv(equipment_data, 'equipments', headers)
        return equipment_data
