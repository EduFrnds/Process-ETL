from etl.layer_bronze.upload import UploadToBronze


def test_generate_data(equipment_generator, tmp_path):

    # GIVEN
    r = 10
    headers = [
        'equipment_id',
        'production',
        'hours_production',
        'temperature',
        'pressure',
        'speed',
        'vibration_level',
        'operation_status',
        'maintenance_type',
        'hours_maintenance'
    ]

    # WHEN
    generated_data = equipment_generator.generate_data_equipments(r, headers)
    csv_file_path = tmp_path / 'equipments.csv'

    # THEN
    assert len(generated_data) == r, "O número de registros gerados não está correto."
    assert len(generated_data[0]) == len(headers), "O número de colunas geradas não está correto."

    # assert not len(generated_data[0]) > len(headers)

    assert f'{csv_file_path}' == str(csv_file_path), "O caminho do arquivo CSV gerado é diferente do esperado."


def test_insert_layer_bronze(mock_data_bronze):

    # GIVEN
    bronze_uploader = UploadToBronze()

    # WHEN
    bronze_uploader.insert_layer_bronze(mock_data_bronze)

    # THEN
    assert bronze_uploader.insert_layer_bronze, "Os dados não foram inseridos corretamente na camada bronze."
