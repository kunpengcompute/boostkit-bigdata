import os

from common.constant import check_oa_conf
from omniadvisor.interface import hijack_recommend
from omniadvisor.utils.logger import global_logger, disable_console_handler


if __name__ == '__main__':
    # 前台劫持功能为了保证用户无感 不应在控制台有日志输出 因此禁用控制台输出
    disable_console_handler(global_logger)
    try:
        check_oa_conf()
        hijack_recommend.main()
    # 无需进行逻辑处理的异常，直接抛至该层
    # 若需进行逻辑处理（如环境清理等），则需在相应位置处理后重新抛至该层
    except Exception as e:
        # 异常信息统一在此处打印堆栈，方便定位，抛出异常的地方无需打印log
        global_logger.exception(e)
        # 异常退出
        # 劫持任务异常退出后，需在Spark脚本内执行原Spark命令语句
        # 选择os._exit()方式退出，避免主进程等待子进程获取Trace的阻塞
        os._exit(1)

    # 正常退出
    global_logger.info('Hijack mission complete!')
    # 选择os._exit()方式退出，避免主进程等待子进程获取Trace的阻塞
    os._exit(0)
