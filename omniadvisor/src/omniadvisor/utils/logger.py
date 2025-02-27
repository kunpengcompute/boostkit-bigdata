import logging
from logging.config import dictConfig
import os

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
            'filename': 'logs/app.log',
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

# 获取当前脚本所在的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))

# 将相对路径转换为绝对路径
absolute_log_path = os.path.abspath(os.path.join(project_dir, LOGGING_CONFIG['handlers']['fileHandler']['filename']))

# 更新配置文件中的路径
LOGGING_CONFIG['handlers']['fileHandler']['filename'] = absolute_log_path
if not os.path.exists(absolute_log_path):
    # 如果文件不存在，则创建空文件
    open(absolute_log_path, 'w').close()
    logging.debug(f"文件 '{absolute_log_path}' 已创建。")
else:
    logging.debug(f"文件 '{absolute_log_path}' 已存在。")

# 使用dictConfig加载配置
dictConfig(LOGGING_CONFIG)

# 获取logger并使用
logger = logging.getLogger(__name__)


def get_logger():
    return logger

