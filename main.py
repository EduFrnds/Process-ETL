import logging

from etl.generate_data import IoTDataGenerator


def generate_iot_data():
    # 1. Geração de dados (Parquet)
    try:
        logging.info("Iniciando o processo ETL...")
        generate_data = IoTDataGenerator()
        generate_data.generate_data_equipaments(1, 20)
        print('equipamento 1 - geração concluída')
        generate_data.generate_data_equipaments(2, 20)
        print('equipamento 2 - geração concluída')
        logging.info("Dados inseridos com sucesso no MongoDB.")
    except Exception as e:
        logging.error(e)

if __name__ == '__main__':
    etl_process = IoTDataGenerator()
    generate_iot_data()

