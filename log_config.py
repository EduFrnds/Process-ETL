import os
import logging
import logging.config


def logging_data(log_dir='logs', log_file_name='process-etl.log'):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, log_file_name)

    # Configuração de logging
    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'file': {
                'level': 'INFO',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': log_file_path,
                'maxBytes': 1024 * 1024 * 5,
                'backupCount': 5,
                'formatter': 'verbose',
                'encoding': 'utf-8',
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
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
                'format': '{levelname} -> {asctime} {module} : {message}',
                'style': '{',
            },
        },
    }

    # Aplica a configuração de logging
    logging.config.dictConfig(LOGGING_CONFIG)
