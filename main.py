import logging

from etl.data_manager import BaseLoader, DataManager
from etl.generate_data import EquipmentProductionDataGenerator
from etl.layer_bronze.upload import UploadToBronze
from etl.layer_gold.transformation import DataTransformationGold
from etl.layer_gold.upload import UploadToGold
from etl.layer_silver.transformation import DataTransformationSilver
from etl.layer_silver.upload import UploadToSilver
# from gcp.gcp_storage import ManagerGCP

from logs.log_config import logging_data


def create_data_csv():
    generate_data_equipment = EquipmentProductionDataGenerator()

    equipment_headers = [
        'equipment_id', 'production', 'hours_production', 'temperature',
        'pressure', 'speed', 'vibration_level', 'operation_status',
        'maintenance_type', 'hours_maintenance'
    ]

    logger = logging.getLogger('process-etl')
    logger.info('Iniciando o Processo de Geração de Dados.')

    try:
        generate_data_equipment.generate_data_equipments(
            1, equipment_headers, 2022, 2023
        )
        logger.info('Processo de Geração de Dados concluído. \n')
    except Exception as e:
        logger.error(f'Erro ao gerar dados: {e}')


def process_silver_layer():
    try:
        # Contador de tempo
        # start_time = datetime.now()

        # Inicializando o processo de transformação da camada Silver
        logging.info("Iniciando o processo de transformação da camada Silver.")

        # Instanciando a classe de transformação com o nome da tabela Bronze e o caminho da camada Silver
        transformation = DataTransformationSilver('layer_bronze.bronze_data', './data')

        # Limpando os dados
        logging.info("Limpando os dados da camada Bronze.")
        df_clean = transformation.clean_data()

        # Agregando os dados
        logging.info("Agregando os dados da camada Bronze.")
        df_aggregated = transformation.aggregate_data(df_clean)

        # Filtrando os dados
        logging.info("Filtrando os dados da camada Bronze.")
        df_filtered = transformation.filter_data(df_aggregated)

        # Derivando os dados e salvando na camada Silver
        headers_silver_data = [
            'equipment_id', 'production', 'hours_production', 'maintenance_type'
            'maintenance_minutes', 'production_minutes', 'temperature_mean',
            'vibration_level_mean', 'temperature_std', 'vibration_level_std',
            'temperature_lsc', 'temperature_lsi', 'vibration_level_lsc',
            'vibration_level_lsi', 'temperature', 'pressure', 'vibration_level'
        ]
        logging.info("Derivando dados e salvando na camada Silver.")

        transformation.derive_data(df_filtered, headers_silver_data)

        # Parar o cronômetro e calcular o tempo total
        # end_time = datetime.now()
        # total_time = end_time - start_time

        logging.info("Processo de transformação da camada Silver concluído com sucesso.\n")
    except Exception as e:
        logging.error(f"Erro durante o processo de transformação: {e}")


def process_gold_layer():
    try:
        # Inicializa o processo de logging
        logging.info("Iniciando o processo de transformação da camada Gold.")

        # Instanciar a classe de transformação da camada Gold
        transformation_gold = DataTransformationGold('layer_silver.silver_data', './data')

        # Definir os headers para a camada Gold
        headers_gold_data = [
            'equipment_id', 'production', 'hours_production', 'maintenance_type',
            'maintenance_minutes', 'production_minutes', 'temperature_mean',
            'vibration_level_mean', 'temperature_std', 'vibration_level_std',
            'temperature_lsc', 'temperature_lsi', 'vibration_level_lsc',
            'vibration_level_lsi', 'temperature', 'vibration_level', 'pressure'
        ]

        # Derivando os dados e salvando na camada Gold
        logging.info("Derivando dados e salvando na camada Gold.")

        # Agregar os dados da camada Silver e salvá-los na camada Gold
        transformation_gold.aggregate_data(transformation_gold.df, headers_gold_data)

        # Salvar os dados na camada Gold
        logging.info("Processo de transformação da camada Gold concluído com sucesso.\n")

    except Exception as e:
        logging.error(f"Erro durante o processo de transformação da camada Gold: {e}")


if __name__ == '__main__':

    logging_data('./logs', 'process-etl.log')
    create_data_csv()
    BaseLoader.load_csv_and_insert('data/equipments.csv', UploadToBronze, 'insert_layer_bronze')

    process_silver_layer()
    BaseLoader.load_csv_and_insert('data/silver_data.csv', UploadToSilver, 'insert_layer_silver')

    process_gold_layer()
    BaseLoader.load_csv_and_insert('data/gold_data.csv', UploadToGold, 'insert_layer_gold')

    # Instancia a classe ManagerGCP
    # manager_gcp = ManagerGCP()
    #
    # # **Abrir o arquivo** para passar um objeto de arquivo no lugar de uma string:
    # with open('./data/equipments.csv', 'rb') as f:
    #     manager_gcp.upload_to_gcs(
    #         bucket_name='bkt-etl-project',
    #         file=f,
    #         file_name='equipments.csv',
    #         content_type='text/csv'
    #     )

    # Deleta os arquivos gerados
    DataManager('./data').delete_files('./data')

    logging.info('Processo concluído.')
