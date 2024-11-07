import pytest
from unittest.mock import patch


@pytest.fixture
def mock_data_bronze():
    PATCH_MOCK = 'etl.layer_bronze.upload.UploadToBronze.insert_layer_bronze'

    with patch(PATCH_MOCK) as mock:
        mock.return_value = {
            'equipment_id': 1,
            'production': 100.5,
            'hours_production': '2023-10-31 08:30:00',
            'temperature': 250.5,
            'pressure': 150.0,
            'speed': 75.2,
            'vibration_level': 1.5,
            'operation_status': 'ON',
            'maintenance_type': 'Repair',
            'hours_maintenance': 3.5
        }
        yield mock


@pytest.fixture
def db_config_mock():
    return {
        "dbname": "process_etl",
        "user": "seu_usuario",
        "password": "sua_senha",
        "host": "localhost",
        "port": "5432"
    }
