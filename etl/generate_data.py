from datetime import datetime, timedelta

import pandas as pd
import random
from faker import Faker

from etl.data_manager import DataManager


class BaseDataGenerator:
    def __init__(self, data_path):
        self.fake = Faker('pt-BR')
        self.data_manager = DataManager(data_path)


class IoTDataGenerator(BaseDataGenerator):

    def __init__(self):
        super().__init__('./data')

    def generate_data_equipaments(self, equipment_id, n=1000):
        """Generate data for equipaments"""
        data = []
        for _ in range(n):
            data.append({
                "equipment_id": equipment_id,
                "production": random.uniform(300.0, 500.0),  # Produção entre 300 e 500 kg/h
                "timestamp": self.fake.date_time_between(start_date='-1d', end_date='now'),
                "temperature": random.uniform(150.0, 250.0),  # Temperatura entre 150°C e 250°C
                "pressure": random.uniform(100.0, 300.0),  # Pressão entre 100 e 300 PSI
                "speed": random.uniform(50.0, 120.0),  # Velocidade entre 50 e 120 RPM
                "vibration_level": random.uniform(0.1, 5.0),  # Nível de vibração entre 0.1 e 5.0 mm/s
                "operation_status": random.choice(["ON", "OFF"])  # Equipamento ligado/desligado
            })
        self.data_manager.save_to_parquet(data, 'equipaments')
        return data


class EquipmentMaintenanceDataGenerator(BaseDataGenerator):
    def __init__(self):
        super().__init__('./data')

    def generate_data_maintenances(self, equipment_id, n=1000):
        """Generate data for maintenances"""
        maintenance_data = []
        for _ in range(n):

            hours_worked = random.uniform(50, 200)  # Exemplo: entre 50 e 200 horas
            productivity = random.uniform(70, 100)  # Produtividade entre 70% e 100%

            maintenance_data.append({
                "equipment_id": equipment_id,
                "maintenance_type": random.choice(["Repair", "Maintenance", "Replace"]),
                "hours_worked": round(hours_worked, 2),
                "productivity": round(productivity, 2),
            })
        self.data_manager.save_to_parquet(maintenance_data, 'maintenances')
        return maintenance_data
