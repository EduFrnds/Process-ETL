import pytest


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
