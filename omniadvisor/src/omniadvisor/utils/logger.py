import logging
from logging.config import dictConfig

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'colorFormatter': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d in %(funcName)s - %(message)s%(reset)s',
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            },
        },
        'simpleFormatter': {
            'format': '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d in %(funcName)s - %(message)s',
        }
    },
    'handlers': {
        'consoleHandler': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'colorFormatter',
            'stream': 'ext://sys.stdout',
        },
        'fileHandler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'INFO',
            'formatter': 'simpleFormatter',
            'filename': '../logs/app.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 7,
            'encoding': 'utf-8',
        }
    },
    'loggers': {
        '': {  # 这是root logger
            'level': 'DEBUG',
            'handlers': ['consoleHandler', 'fileHandler'],
        }
    }
}

# 使用dictConfig加载配置
dictConfig(LOGGING_CONFIG)

# 获取logger并使用
logger = logging.getLogger(__name__)


def get_logger():
    return logger

