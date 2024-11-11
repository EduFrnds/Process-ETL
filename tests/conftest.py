import csv

import pytest
from unittest.mock import patch

from etl.generate_data import EquipmentProductionDataGenerator


@pytest.fixture
def mock_data_bronze():
    """
    Mock para os dados da camada bronze.
    """
    file_path = './data_test/equipments.csv'
    data = []

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)

    return data


@pytest.fixture
def equipment_generator(tmp_path):
    """
    Fixture para inicializar o gerador de dados.
    """
    generator = EquipmentProductionDataGenerator()
    generator.data_manager.output_dir = tmp_path
    return generator


@pytest.fixture
def db_config_mock():
    """
    Mock para a configuração do banco de dados.
    """
    return {
        "dbname": "process_etl",
        "user": "seu_usuario",
        "password": "sua_senha",
        "host": "localhost",
        "port": "5432"
    }
