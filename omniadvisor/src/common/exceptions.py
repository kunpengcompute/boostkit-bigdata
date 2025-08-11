class NoOptimalConfigError(Exception):
    """
    异常，调优未能推荐有效配置
    """
    pass


class TuningPreconditionError(Exception):
    """
    未满足调优前置条件
    """
    pass


class SystemKilledError(Exception):
    """
    异常，程序被异常中断，例如kill等
    """
    pass


class DuplicateEntryError(Exception):
    """
    数据重复异常
    """
    pass


class UnknownEncodingError(Exception):
    """
    未知编码异常
    """
    pass
