import os
import logging
from logging.config import dictConfig
from common.constant import OA_CONF

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'colorFormatter': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d in %(funcName)s - %(message)s%(reset)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
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
            'datefmt': '%Y-%m-%d %H:%M:%S'
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
            'filename': OA_CONF.log_file_path,
            'when': 'midnight',
            'interval': 1,
            'backupCount': 7,
            'encoding': 'utf-8',
        }
    },
    'loggers': {
        'project_logger': {
            'level': 'DEBUG',
            'handlers': ['consoleHandler', 'fileHandler'],
        }
    }
}

# 检查并创建log文件夹
if not os.path.exists(OA_CONF.log_dir):
    os.makedirs(OA_CONF.log_dir)

# 使用dictConfig加载配置
dictConfig(LOGGING_CONFIG)
# 获取logger并使用
logger = logging.getLogger('project_logger')
