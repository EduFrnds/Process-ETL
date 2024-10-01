import logging

from log_config import logging_data

from etl.generate_data import IoTDataGenerator, EquipmentMaintenanceDataGenerator


def generate_data_process():
    # 1. Geração de dados (Parquet)
    generate_data = IoTDataGenerator()
    generate_data_maintenance = EquipmentMaintenanceDataGenerator()

    logger = logging.getLogger('process-etl')  # Obtém o logger que configuramos
    logger.info('Iniciando o processo de Geração de Dados...')

    try:
        generate_data.generate_data_equipaments(1, 1000)
        generate_data_maintenance.generate_data_maintenances(1, 1000)
        print('equipamento 1 - geração concluída')

        generate_data.generate_data_equipaments(2, 1000)
        generate_data_maintenance.generate_data_maintenances(2, 1000)
        print('equipamento 2 - geração concluída')

    except Exception as e:
        logger.error(f'Erro ao executar o processo de Geração de dados: {e}')
    finally:
        logger.info('Processo de Geração de Dados concluído.')


if __name__ == '__main__':
    logging_data()
    etl_process = IoTDataGenerator()
    generate_data_process()
