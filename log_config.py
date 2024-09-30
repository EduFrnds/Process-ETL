import logging
import os


def setup_logging():
    # Cria o diretório de logs se não existir
    log_directory = 'logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Define o formato do log
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        filename=os.path.join(log_directory, 'etl_project.log'),  # Nome do arquivo de log
        filemode='a',  # 'a' para adicionar, 'w' para sobrescrever
        level=logging.INFO,  # Define o nível do log
        format=log_format,
    )

    # Adiciona também um handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Mantenha o nível do console
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(console_handler)
