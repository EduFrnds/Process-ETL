import os
import logging
import logging.config


def logging_data(log_dir='logs', log_file_name='process-etl.log'):
    log_file_path = os.path.join(log_dir, log_file_name)

    # Configuração de logging com rotação de arquivos
    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',  # Handler de rotação
                'filename': log_file_path,
                'maxBytes': 1024 * 1024 * 5,  # Tamanho máximo do arquivo de log (5 MB)
                'backupCount': 5,  # Número máximo de arquivos de backup (5 backups)
                'formatter': 'verbose',
                'encoding': 'utf-8',  # Certifica que os logs serão gravados em UTF-8
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',  # Exibe logs no console
                'formatter': 'verbose',
            },
        },
        'loggers': {
            'process-etl': {
                'handlers': ['file', 'console'],
                'level': 'INFO',
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['file', 'console'],
            'level': 'INFO',
        },
        'formatters': {
            'verbose': {
                'format': '{levelname} -> {asctime} {module} : {message}',  # Formato do log
                'style': '{',  # Suporte para a formatação com chaves {}
            },
        },
    }

    # Aplica a configuração de logging
    logging.config.dictConfig(LOGGING_CONFIG)
