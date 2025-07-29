class NoOptimalConfigError(Exception):
    """
    异常，调优未能推荐有效配置
    """
    pass


class SystemKilledError(Exception):
    """
    异常，程序被异常中断，例如kill等
    """
    pass