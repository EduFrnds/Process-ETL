import pytest
from unittest.mock import patch, MagicMock
from etl.layer_bronze.upload import UploadToBronze


def test_insert_layer_bronze(mock_data_bronze):
    """Testa o método insert_layer_bronze para a camada Bronze usando dados mockados"""

    mock_bronze = UploadToBronze()
    mock_bronze.insert_layer_bronze(mock_data_bronze.return_value)
