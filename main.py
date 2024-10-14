import logging

from etl.data_manager import DataManager
from etl.generate_data import EquipmentProductionDataGenerator
from etl.layer_bronze.load import LoadToBronze
from etl.layer_silver.read import ReadData
from log_config import logging_data


def create_data_csv():

    generate_data_equipment = EquipmentProductionDataGenerator()

    equipment_headers = [
        'equipment_id', 'production', 'hours_production', 'temperature', 'pressure', 'speed', 'vibration_level',
        'operation_status', 'maintenance_type', 'hours_maintenance'
    ]

    logger = logging.getLogger('process-etl')
    logger.info('Iniciando o processo de Geração de Dados.')

    RECORDS_TO_GENERATE = 1500

    try:
        generate_data_equipment.generate_data_equipments(
            RECORDS_TO_GENERATE, equipment_headers
        )
        logger.info('Processo de Geração de Dados concluído.')
    except Exception as e:
        logger.error(f'Erro ao gerar dados: {e}')


def insert_csv():
    logger = logging.getLogger('process-etl')
    logger.info('Iniciando o processo de Inserção de Dados.')
    try:
        conn = LoadToBronze()
        conn.insert_csv('data/equipments.csv')
        logger.info('Processo de Inserção de Dados concluído.')
    except Exception as e:
        logger.error(f'Erro ao inserir dados: {e}')


if __name__ == '__main__':
    logging_data()
    create_data_csv()
    insert_csv()
    DataManager.delete_files('./data')
    read = ReadData()
    read.read_layer_bronze('layer_bronze')
