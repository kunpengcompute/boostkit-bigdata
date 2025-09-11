import os
import logging
import warnings
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler

from common.constant import OA_CONF

CONSOLE_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
FILE_FORMAT = '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d in %(funcName)s - %(message)s'


class SecureRotatingFileHandler(RotatingFileHandler):
    """
    自定义RotatingFileHandler，支持设置文件权限
    """
    def _open(self):
        """
        打开文件并设置权限
        """
        stream = super()._open()
        # 正在记录的日志文件权限设置为640
        os.chmod(self.baseFilename, 0o640)
        return stream

    def doRollover(self):
        """
        执行日志滚动并设置归档文件权限
        """
        # 调用父类方法执行滚动
        super().doRollover()

        # 设置所有归档文件的权限
        self._set_archive_permissions()

    def _set_archive_permissions(self):
        """
        设置所有归档日志文件的权限为440
        """
        # 获取日志目录和基本文件名
        dir_name, base_name = os.path.split(self.baseFilename)

        # 获取所有可能的归档文件
        file_names = os.listdir(dir_name)
        for file_name in file_names:
            if file_name.startswith(base_name) and file_name != os.path.basename(self.baseFilename):
                archive_path = os.path.join(dir_name, file_name)
                # 归档的日志文件权限设置为440
                os.chmod(archive_path, 0o440)


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
            '()': SecureRotatingFileHandler,
            'level': 'DEBUG',
            'formatter': 'fileFormatter',
            'filename': OA_CONF.log_file_path,
            'maxBytes': 100 * 1024 * 1024,  # 单个文件最大 100MB
            'backupCount': 20,              # 保留 20 个旧文件，超过就覆盖最早的
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
    # 日志文件目录权限设置为750
    os.chmod(OA_CONF.log_dir, 0o750)

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