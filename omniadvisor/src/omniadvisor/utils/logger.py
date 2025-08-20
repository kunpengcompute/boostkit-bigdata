import os
import logging
import warnings
from logging.config import dictConfig
from common.constant import OA_CONF

CONSOLE_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
FILE_FORMAT = '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d in %(funcName)s - %(message)s'

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'consoleFormatter': {
            '()': 'colorlog.ColoredFormatter',
            'format': f'%(log_color)s{CONSOLE_FORMAT}%(reset)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'log_colors': {
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            },
        },
        'fileFormatter': {
            'format': FILE_FORMAT,
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'consoleHandler': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'consoleFormatter',
            'stream': 'ext://sys.stdout',
        },
        'fileHandler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'fileFormatter',
            'filename': OA_CONF.log_file_path,
            'when': 'midnight',
            'interval': 1,
            'backupCount': 7,
            'encoding': 'utf-8',
        }
    },
    'loggers': {
        'omniadvisor': {
            'level': 'INFO',
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
global_logger = logging.getLogger('omniadvisor')
# 禁止logger作用域上升到root
global_logger.propagate = False

# 屏蔽warnings模块中的RuntimeWarning 避免smac库在运行时偶发的warning打印影响用户体验
warnings.filterwarnings('ignore')

# 屏蔽部分第三方库中的Logger
modules_setting_error = ['smac']
for model in modules_setting_error:
    logging.getLogger(model).setLevel(logging.ERROR)


def disable_console_handler(logger):
    """
    如果logger中存在consoleHandler 将consoleHandler移除

    :param logger: 目标 logger 实例
    """
    for handler in logger.handlers[:]:
        if getattr(handler, "name", None) == 'consoleHandler':
            logger.removeHandler(handler) # 禁用 consoleHandler
            return
    logger.debug("consoleHandler not exist, no need to disable")