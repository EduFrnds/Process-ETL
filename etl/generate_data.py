from datetime import datetime

import pandas as pd
import random
from faker import Faker

from etl.data_manager import DataManager


class IoTDataGenerator:

    def __init__(self):
        self.fake = Faker('pt-BR')
        self.data_manager = DataManager('./data')

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


