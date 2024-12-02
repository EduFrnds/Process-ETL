import pandas as pd

from unittest.mock import patch, MagicMock

from etl.generate_data import EquipmentProductionDataGenerator
from etl.layer_bronze.upload import UploadToBronze
from etl.layer_silver.read import ReadDataBronze
from etl.layer_silver.transformation import DataTransformationSilver


def test_generate_data():
    # GIVEN:
    generator = EquipmentProductionDataGenerator()
    r = 10
    headers = [
        'equipment_id', 'production', 'hours_production', 'temperature',
        'pressure', 'speed', 'vibration_level', 'operation_status',
        'maintenance_type', 'hours_maintenance'
    ]

    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
        # WHEN: O método de geração de dados é chamado
        equipment_data = generator.generate_data_equipments(r, headers)

        # THEN: Verifica se os dados foram gerados corretamente
        assert len(equipment_data) == r, "O número de linhas geradas não é igual ao esperado"
        assert list(equipment_data.columns) == headers, "As colunas geradas não correspondem aos headers fornecidos"

        # AND: Verifica se o método to_csv foi chamado corretamente
        mock_to_csv.assert_called_once_with('./data/equipments.csv', index=False)


def test_insert_layer_bronze():
    # GIVEN: Uma instância de UploadToBronze e um row de exemplo
    uploader = UploadToBronze()
    row = {
        'equipment_id': '1',
        'production': '200.5',
        'hours_production': '2023-11-20 12:30:00',
        'temperature': '220.5',
        'pressure': '2.3',
        'speed': '50.0',
        'vibration_level': '0.5',
        'operation_status': 'ON',
        'maintenance_type': 'Repair',
        'hours_maintenance': '5.0'
    }

    schema = {
        'equipment_id': int,
        'production': float,
        'hours_production': str,
        'temperature': float,
        'pressure': float,
        'speed': float,
        'vibration_level': float,
        'operation_status': str,
        'maintenance_type': str,
        'hours_maintenance': float
    }

    expected_processed_row = [
        1, 200.5, '2023-11-20 12:30:00', 220.5, 2.3, 50.0, 0.5, 'ON', 'Repair', 5.0
    ]

    # Mock dos métodos dependentes (process_row e insert_data)
    with patch.object(uploader, 'insert_data') as mock_insert_data, \
            patch.object(uploader, 'process_row', return_value=expected_processed_row) as mock_process_row:
        # WHEN: O método insert_layer_bronze é chamado
        uploader.insert_layer_bronze(row)

        # THEN: Verifica se process_row foi chamado corretamente
        mock_process_row.assert_called_once_with(row, schema)

        # AND: Verifica se insert_data foi chamado com os parâmetros corretos
        mock_insert_data.assert_called_once_with(
            'layer_bronze.bronze_data',
            list(schema.keys()),  # Colunas do schema
            expected_processed_row  # Valores processados esperados
        )


def test_read_layer_bronze():
    # GIVEN: Uma instância de ReadDataBronze e os mocks necessários
    reader = ReadDataBronze()
    table_name = "layer_bronze.bronze_data"

    # Dados simulados que seriam retornados pelo banco
    mock_data = [
        (1, 200.5, '2023-11-20 12:30:00', 220.5, 2.3, 50.0, 0.5, 'ON', 'Repair', 5.0),
        (2, 150.0, '2023-11-19 10:00:00', 210.0, 2.1, 45.0, 0.3, 'OFF', 'Maintenance', 4.0)
    ]
    mock_columns = [
        'equipment_id', 'production', 'hours_production', 'temperature',
        'pressure', 'speed', 'vibration_level', 'operation_status',
        'maintenance_type', 'hours_maintenance'
    ]

    # Mock do cursor e dos métodos executados
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_data
    mock_cursor.description = [(col,) for col in mock_columns]

    # Mock da conexão e do cursor
    with patch.object(reader, 'cur', mock_cursor):
        # WHEN: O método read_layer_bronze é chamado
        result = reader.read_layer_bronze(table_name)

        # THEN: Verifica se a query foi executada corretamente
        mock_cursor.execute.assert_called_once_with("SELECT * FROM layer_bronze.bronze_data")

        # AND: Verifica se o resultado foi transformado em DataFrame corretamente
        expected_df = pd.DataFrame(mock_data, columns=mock_columns)
        pd.testing.assert_frame_equal(result, expected_df)


def test_derive_data():
    # GIVEN: Dados de entrada fictícios
    input_data = {
        'equipment_id': [1, 1, 2, 2],
        'temperature': [220.0, 230.0, 240.0, 250.0],
        'pressure': [2.3, 2.5, 2.6, 2.7],
        'vibration_level': [0.5, 0.6, 0.7, 0.8],
    }
    headers = ['equipment_id', 'temperature_mean', 'vibration_level_mean',
               'temperature_std', 'vibration_level_std', 'temperature_lsc',
               'temperature_lsi', 'vibration_level_lsc', 'vibration_level_lsi', 'temperature']

    df_input = pd.DataFrame(input_data)

    # Resultado esperado após as transformações
    expected_data = {
        'equipment_id': [1, 1, 2, 2],
        'temperature': [220.0, 230.0, 240.0, 250.0],
        'temperature_mean': [225.0, 225.0, 245.0, 245.0],
        'vibration_level_mean': [0.55, 0.55, 0.75, 0.75],
        'temperature_std': [7.07, 7.07, 7.07, 7.07],
        'vibration_level_std': [0.07, 0.07, 0.07, 0.07],
        'temperature_lsc': [246.21, 246.21, 266.21, 266.21],
        'temperature_lsi': [203.79, 203.79, 223.79, 223.79],
        'vibration_level_lsc': [0.76, 0.76, 0.96, 0.96],
        'vibration_level_lsi': [0.34, 0.34, 0.54, 0.54]
    }
    df_expected = pd.DataFrame(expected_data)
    # print(df_expected) - Imprime o DataFrame esperado, validando o método.

    # Mock do método to_csv para evitar escrita real no sistema de arquivos
    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
        # WHEN: O método derive_data é chamado
        result = DataTransformationSilver.derive_data(df_input, headers)

        # THEN: Verifica se o DataFrame resultante está correto
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df_expected.reset_index(drop=True))

        # AND: Verifica se o método to_csv foi chamado corretamente
        mock_to_csv.assert_called_once_with('./data/silver_data.csv', index=False)
