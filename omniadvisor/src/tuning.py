from omniadvisor.interface import config_tuning
from omniadvisor.utils.logger import global_logger


if __name__ == '__main__':
    try:
        config_tuning.main()
    # 无需进行逻辑处理的异常，直接抛至该层
    # 若需进行逻辑处理（如环境清理等），则需在相应位置处理后重新抛至该层
    except Exception as e:
        # 异常信息统一在此处打印堆栈，方便定位，抛出异常的地方无需打印log
        global_logger.exception(e)
        # 异常退出
        exit(1)

    # 正常退出
    global_logger.info('Tuning mission complete!')
    exit(0)
