import logging

from etl.generate_data import EquipmentProductionDataGenerator, EquipmentMaintenanceDataGenerator
from etl.layer_bronze.bronze import LoadToBronze
from log_config import logging_data


def create_data_csv():
    """
    Genarate data for equipaments and maintenances and save them in CSV files.
    """
    generate_data_equipment = EquipmentProductionDataGenerator()
    generate_data_maintenance = EquipmentMaintenanceDataGenerator()

    equipment_headers = [
        'equipment_id', 'production', 'timestamp', 'temperature', 'pressure', 'speed', 'vibration_level',
        'operation_status'
    ]
    maintenance_headers = [
                'equipment_id', 'maintenance_type', 'hours_maintenance'
    ]

    logger = logging.getLogger('process-etl')
    logger.info('Iniciando o processo de Geração de Dados...')

    RECORDS_TO_GENERATE = 1000

    try:
        generate_data_equipment.generate_data_equipments(
            RECORDS_TO_GENERATE, equipment_headers
        )
        generate_data_maintenance.generate_data_maintenances(
            RECORDS_TO_GENERATE, maintenance_headers
        )
        logger.info('Processo de Geração de Dados concluído.')
    except Exception as e:
        logger.error(f'Erro ao gerar dados: {e}')


def upload_layer_bronze():
    """
    Upload data to layer bronze.
    """
    load_to_bronze = LoadToBronze(
        'data',
        'maintenances',
        'postgresql://postgres:postgres@localhost:5432/postgres'
    )


if __name__ == '__main__':
    logging_data()
    create_data_csv()
    print("CSV generation complete!")
    upload_layer_bronze()
    print("Data uploaded to layer bronze!")
